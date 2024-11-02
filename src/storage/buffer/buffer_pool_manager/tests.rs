use super::*;
use crate::common::constants::{INVALID_PID, NEW_PAGE_ERR_MSG, NO_CORRESPONDING_PAGE_MSG};
use crate::storage::disk::disk_manager::{DiskManager, PageId};
use crate::storage::page::RecordId;
use crate::storage::page::{Page, TablePageHandle};
use crate::storage::tuple::{Tuple, TupleMetadata};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[test]
fn test_new_page_basic() {
    let mut bpm = get_bpm_with_pool_size(5);

    let page_id = bpm.new_page().unwrap();
    let page = get_page_handle(&bpm, &page_id).unwrap();
    let page_guard = page.read().unwrap();

    // new page correctly initialized.
    assert_eq!(page_id, 1);
    assert_eq!(page_id, *page_guard.page_id());

    // page inserted into buffer pool, and pinned to prevent eviction.
    assert!(page_in_buffer(&bpm, &page_id));
    assert_eq!(bpm.get_pin_count(&page_id).unwrap(), 1);
}

#[test]
fn test_new_page_no_initial_frames() {
    let mut bpm = get_bpm_with_pool_size(0);
    assert!(bpm.new_page().is_none());
}

#[test]
fn test_cannot_create_page_beyond_buffer_pool_size() {
    let mut bpm = get_bpm_with_pool_size(2);

    // Create and pin two pages.
    let page_id1 = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    let page_id2 = bpm.new_page().expect(NEW_PAGE_ERR_MSG);

    bpm.fetch_page(&page_id1);
    bpm.fetch_page(&page_id2);

    // All frames are now pinned, attempt to create another page.
    let result = bpm.new_page();
    assert!(result.is_none());
}

#[test]
fn test_new_page_evict_frame() {
    let pool_size = 3_usize;
    let mut bpm = get_bpm_with_pool_size(pool_size);

    let mut new_page_id: Option<PageId> = None;
    for _ in 0..pool_size {
        assert!(!bpm.free_list.is_empty());
        new_page_id = bpm.new_page();
        assert!(new_page_id.is_some());
    }

    // free list empty, and no evictable page.
    assert!(bpm.free_list.is_empty());
    assert!(bpm.new_page().is_none());

    // free list empty, but there's an evictable page.
    let page_id_to_evict = &new_page_id.unwrap();
    {
        let binding = bpm.replacer.clone();
        let mut replacer = binding.write().unwrap();
        bpm.set_evictable(page_id_to_evict, true, &mut replacer);
    }
    assert!(bpm.free_list.is_empty());
    let new_page_after_eviction = bpm.new_page();
    assert!(new_page_after_eviction.is_some());

    assert!(bpm.free_list.is_empty());
    assert!(bpm.new_page().is_none());
}

#[test]
fn test_fetch_page_in_buffer() {
    let pool_size = 10_usize;
    let mut bpm = get_bpm_with_pool_size(pool_size);

    let page_ids = create_n_pages(&mut bpm, pool_size);
    page_ids
        .iter()
        .for_each(|&page_id| assert_eq!(fetch_page_get_id(&page_id, &mut bpm), page_id));
}

/// This test assumes [`super::BufferPoolManager::unpin_page`] functions properly.
#[test]
fn test_fetch_page_not_in_buffer() {
    let pool_size = 10_usize;
    let mut bpm = get_bpm_with_pool_size(pool_size);

    // fill buffer pool to capacity with new page.
    let page_id_to_evict = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    bpm.unpin_page(&page_id_to_evict, false);
    create_n_pages(&mut bpm, pool_size - 1);

    // and add another page.
    let another_page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    bpm.unpin_page(&another_page_id, false); // for the fetch_page later

    // verify a page was evicted for the new page.
    assert!(!bpm.page_table.contains_key(&page_id_to_evict));

    // ...we should still be able to fetch that evicted page (from disk).
    assert_eq!(
        fetch_page_get_id(&page_id_to_evict, &mut bpm),
        page_id_to_evict
    );

    // another fetch of that page (this time from the buffer pool!)
    assert_eq!(
        fetch_page_get_id(&page_id_to_evict, &mut bpm),
        page_id_to_evict
    );
}

#[test]
fn test_unpin_page_changes_dirty_flag() {
    let mut bpm = get_bpm_with_pool_size(5);
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);

    assert!(!bpm.get_is_dirty(&page_id));
    assert!(bpm.unpin_page(&page_id, true));
    assert!(bpm.get_is_dirty(&page_id));
}

#[test]
#[should_panic]
fn test_unpin_page_not_in_buffer_pool() {
    let mut bpm = get_bpm_with_pool_size(0);
    // buffer pool is empty
    assert!(!bpm.unpin_page(&INVALID_PID, false));
}

/// This tests assumes [`super::BufferPoolManager::delete_page`] functions properly.
#[test]
fn test_unpin_page_before_and_after_deletion() {
    let mut bpm = get_bpm_with_pool_size(5);

    // Pin count: 1
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);

    // Pin count: 0
    assert!(bpm.unpin_page(&page_id, false));

    // Pin count: still 0
    assert!(!bpm.unpin_page(&page_id, false));
    assert!(bpm.delete_page(page_id));
}

/// This tests assumes [`super::BufferPoolManager::fetch_page`] properly increments pin count.
#[test]
fn test_unpin_page_decrements_multiple_times() {
    let mut bpm = get_bpm_with_pool_size(5);

    // Pin count: 1
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    // Pin count: 26
    for _ in 0..25 {
        bpm.fetch_page(&page_id);
    }
    assert_eq!(bpm.get_pin_count(&page_id).unwrap(), 26);

    // Pin count: 25 -> 24 -> ... -> 0
    for i in (0..26).rev() {
        assert!(bpm.unpin_page(&page_id, false));
        assert_eq!(bpm.get_pin_count(&page_id).unwrap(), i);
    }
}

#[test]
#[should_panic]
fn test_flush_page_does_not_exist() {
    let mut bpm = get_bpm_with_pool_size(5);
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    let different_page_id = page_id + 1;
    bpm.flush_page(&different_page_id);
}

#[test]
fn test_flush_page() {
    // should be able to flush page regardless of is_dirty flag
    [true, false].iter().for_each(|&is_dirty| {
        let mut bpm = get_bpm_with_pool_size(5);
        let unevictable_page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
        let evictable_page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
        {
            let binding = bpm.replacer.clone();
            let mut replacer = binding.write().unwrap();
            bpm.set_evictable(&evictable_page_id, true, &mut replacer);
        }

        bpm.set_is_dirty(&unevictable_page_id, is_dirty);
        bpm.set_is_dirty(&evictable_page_id, is_dirty);

        bpm.flush_page(&unevictable_page_id);
        bpm.flush_page(&evictable_page_id);

        // is_dirty flag should be reset to false after page flush
        assert!(!bpm.get_is_dirty(&unevictable_page_id));
        assert!(!bpm.get_is_dirty(&evictable_page_id));
    })
}

#[test]
#[should_panic]
fn test_flush_all_pages_but_page_does_not_exist() {
    let pool_size = 5;
    let mut bpm = get_bpm_with_pool_size(pool_size);

    let page_ids: Vec<PageId> = create_n_pages(&mut bpm, pool_size);
    let different_page_id = create_different_page_id(&page_ids);

    bpm.flush_page(&different_page_id);
}

#[test]
fn test_flush_all_pages() {
    let pool_size = 1000;
    let mut bpm = get_bpm_with_pool_size(pool_size);

    let page_ids: Vec<PageId> = create_n_pages(&mut bpm, pool_size);
    set_pages_to_dirty(&mut bpm, &page_ids);

    page_ids.iter().for_each(|page_id| {
        bpm.flush_page(page_id);
        assert!(!bpm.get_is_dirty(page_id));
    })
}

#[test]
#[should_panic]
fn test_delete_page_does_not_exist() {
    let mut bpm = get_bpm_with_pool_size(5);
    let page_id = bpm
        .new_page()
        .expect("There was an error creating a new page.");
    let different_page_id = page_id + 1;
    bpm.delete_page(different_page_id);
}

#[test]
fn test_cannot_delete_pinned_page() {
    let mut bpm = get_bpm_with_pool_size(5);
    // this is pinned in the buffer pool, shouldn't be able to delete
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    assert!(!bpm.delete_page(page_id));
}

/// This tests assumes [`super::BufferPoolManager::unpin_page`] properly decrements pin count.
#[test]
fn test_delete_evictable_page() {
    let mut bpm = get_bpm_with_pool_size(5);
    let page_id = bpm.new_page().expect(NEW_PAGE_ERR_MSG);

    bpm.unpin_page(&page_id, false);
    assert!(bpm.delete_page(page_id));
    assert!(!bpm.page_table.contains_key(&page_id));
}

/// This tests assumes [`super::BufferPoolManager::unpin_page`] properly decrements pin count.
#[test]
fn test_attempt_deletion_of_evictable_and_pinned_pages() {
    let pool_size = 20_usize;
    let mut bpm = get_bpm_with_pool_size(pool_size);
    let page_ids = create_n_pages(&mut bpm, pool_size);

    // set half the page to evictable; the other half remain pinned
    let evictable_page_ids =
        set_pages_satisfying_criteria_to_evictable(&mut bpm, &page_ids, page_number_is_even);

    for page_id in page_ids {
        let was_deleted = bpm.delete_page(page_id.clone());
        let should_have_been_deleted = evictable_page_ids.contains(&page_id);
        assert_eq!(was_deleted, should_have_been_deleted);
    }
}

#[test]
fn test_dirty_pages_eviction() {
    let disk_manager = new_disk_manager();
    let mut bpm = BufferPoolManager::new(2, 5, Arc::clone(&disk_manager));

    // Create and unpin a page.
    let page_id1 = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    let page_handle1 = bpm.fetch_page(&page_id1).expect("Failed to fetch page");
    let tuple = Tuple::from(&b"Northwestern"[..]);
    let tuple_metadata = TupleMetadata::new(false);
    {
        let mut page1 = page_handle1.write().unwrap();
        page1.insert_tuple(tuple_metadata, tuple.clone());
    }
    bpm.unpin_page(&page_id1, true);
    bpm.unpin_page(&page_id1, true);

    // Create and unpin another page.
    let page_id2 = bpm.new_page().expect(NEW_PAGE_ERR_MSG);
    bpm.unpin_page(&page_id2, false);

    // Now the buffer pool is full. Creating a new page will cause eviction.
    let page_id3 = bpm
        .new_page()
        .expect("Should be able to create a new page after eviction");
    bpm.unpin_page(&page_id3, true);

    let page_handle = bpm.fetch_page(&page_id1).expect("Failed to fetch page");
    let page1 = page_handle.write().unwrap();
    let rc1 = RecordId::new(page1.page_id, 0);
    assert_eq!(page1.get_tuple(&rc1).unwrap(), tuple);

    // The dirty page (page_id1) should have been evicted and written to disk.
    // Read the page from disk and verify its contents.
    let page_on_disk = disk_manager.write().unwrap().read_page(&page_id1);
    assert_eq!(
        page_on_disk.get_tuple(&rc1).unwrap(),
        tuple,
        "Data on disk should match data in memory"
    );
}

/// This test is simulating latches and concurrent access to buffer pool manager, but it does
/// not require the buffer pool manager to be implemented in a thread-safe manner internally.
#[test]
fn test_serialized_evictable() {
    const ROUNDS: usize = 50;
    const NUM_READERS: usize = 8;

    // Initialize the disk manager.
    let disk_manager = new_disk_manager();

    // Only allocate 1 frame of memory to the buffer pool manager.
    let bpm = Arc::new(RwLock::new(BufferPoolManager::new(
        1,
        2,
        Arc::clone(&disk_manager),
    )));

    for i in 0..ROUNDS {
        // Use an AtomicBool for synchronization.
        let signal = Arc::new(AtomicBool::new(false));

        // Allocate pages via DiskManager.
        let winner_pid = {
            let mut disk_guard = disk_manager.write().unwrap();
            disk_guard.allocate_new_page()
        };

        let loser_pid = {
            let mut disk_guard = disk_manager.write().unwrap();
            disk_guard.allocate_new_page()
        };

        let mut readers = Vec::new();

        for _ in 0..NUM_READERS {
            let signal = Arc::clone(&signal);
            let bpm = Arc::clone(&bpm);
            let winner_pid = winner_pid;
            let loser_pid = loser_pid;

            let reader = thread::spawn(move || {
                // Wait until the main thread has taken a latch on the page.
                while !signal.load(Ordering::SeqCst) {
                    // Sleep briefly to prevent busy waiting.
                    thread::sleep(Duration::from_millis(1));
                }

                // Fetch and read the page.
                {
                    let mut bpm_guard = bpm.write().unwrap();
                    let _page_handle = bpm_guard.fetch_page(&winner_pid).unwrap();

                    // Since the only frame is pinned, no thread should be able to bring in a new page.
                    let result = bpm_guard.fetch_page(&loser_pid);
                    assert!(result.is_none());

                    // Unpin the page after use.
                    bpm_guard.unpin_page(&winner_pid, false);
                }
            });

            readers.push(reader);
        }

        match i % 2 {
            0 => {
                let mut bpm_guard = bpm.write().unwrap();
                let page_handle = bpm_guard.fetch_page(&winner_pid).unwrap();

                // Obtain a read lock on the page content.
                let _page_read_lock = page_handle.read().unwrap();

                // Signal all the readers to proceed.
                signal.store(true, Ordering::SeqCst);

                // Allow other threads to read.
                drop(_page_read_lock);

                // Unpin the page.
                bpm_guard.unpin_page(&winner_pid, false);
            }
            _ => {
                let mut bpm_guard = bpm.write().unwrap();
                let page_handle = bpm_guard.fetch_page(&winner_pid).unwrap();

                // Obtain a write lock on the page content.
                let _page_write_lock = page_handle.write().unwrap();

                // Signal all the readers to proceed.
                signal.store(true, Ordering::SeqCst);

                // Allow other threads to read.
                drop(_page_write_lock);

                // Unpin the page.
                bpm_guard.unpin_page(&winner_pid, false);
            }
        }

        for reader in readers {
            reader.join().unwrap();
        }
    }
}

#[test]
fn page_pin_test() {
    // Number of frames in the buffer pool.
    const FRAMES: usize = 10;

    // Initialize the disk manager.
    let disk_manager = new_disk_manager();

    // Initialize the buffer pool manager with 10 frames, and LRU-2.
    let mut bpm = BufferPoolManager::new(FRAMES, 2, Arc::clone(&disk_manager));

    let mut pages: Vec<PageId> = Vec::new();

    // The buffer pool is empty. We should be able to create a new page.
    let pid0 = bpm.new_page().expect("Failed to create a new page.");
    pages.push(pid0);

    // Fetch the page and write "Hello" to it using insert_tuple.
    let rid0;
    {
        let page0_handle = bpm.fetch_page(&pid0).expect("Failed to fetch page0.");
        {
            // Insert "Hello" into the page.
            let mut page0 = page0_handle.write().unwrap();
            let tuple = Tuple::from(b"Hello".to_vec());
            let meta = TupleMetadata::new(false);
            let slot_id = page0
                .insert_tuple(meta, tuple)
                .expect("Failed to insert tuple.");
            rid0 = RecordId::new(pid0, slot_id);
        }
        // Verify that we can read back "Hello."
        {
            let page0 = page0_handle.read().unwrap();
            let tuple = page0.get_tuple(&rid0).expect("Failed to get tuple.");
            assert_eq!(tuple.data, b"Hello", "Data read does not match 'Hello'.");
        }
        // Unpin the page.
        bpm.unpin_page(&pid0, true);
    }

    // We should be able to create new pages until we fill up the buffer pool.
    for _ in 0..FRAMES - 1 {
        let pid = bpm.new_page().expect("Failed to create a new page.");
        // No need to fetch the page here since we're not modifying it.
        pages.push(pid);
    }

    // All pin counts should be 1.
    for pid in &pages {
        let pin_count = bpm.get_pin_count(pid).expect("Failed to get pin count.");
        assert_eq!(pin_count, 1, "Pin count for page {} is not 1.", pid);
    }

    // Once the buffer pool is full, we should not be able to create any new pages.
    for _ in 0..FRAMES {
        let result = bpm.new_page();
        assert!(
            result.is_none(),
            "Expected new_page to return None when buffer pool is full."
        );
    }

    // Drop the first 5 pages to unpin them.
    for _ in 0..(FRAMES / 2) {
        let pid = pages.remove(0);
        bpm.unpin_page(&pid, false);
        // Check that the pin count is now 0.
        let pin_count = bpm.get_pin_count(&pid).expect("Failed to get pin count.");
        assert_eq!(
            pin_count, 0,
            "Pin count for page {} is not 0 after unpinning.",
            pid
        );
    }

    // All pin counts of the pages we haven't dropped yet should still be 1.
    for pid in &pages {
        let pin_count = bpm.get_pin_count(pid).expect("Failed to get pin count.");
        assert_eq!(pin_count, 1, "Pin count for page {} is not 1.", pid);
    }

    // After unpinning pages, we should be able to create new pages and bring them into memory.
    for _ in 0..((FRAMES / 2) - 1) {
        let pid = bpm.new_page().expect("Failed to create a new page.");
        pages.push(pid);
    }

    // There should be one frame available, and we should be able to fetch the data we wrote earlier.
    {
        let page0_handle = bpm.fetch_page(&pid0).expect("Failed to fetch pid0.");
        {
            let page0 = page0_handle.read().unwrap();
            let tuple = page0.get_tuple(&rid0).expect("Failed to get tuple.");
            assert_eq!(
                tuple.data, b"Hello",
                "Data read from pid0 does not match 'Hello'."
            );
        }
        // Unpin the page
        bpm.unpin_page(&pid0, false);
    }

    // Once we unpin page 0 and then make a new page, all the buffer pages should now be pinned.
    // Fetching page 0 again should fail.
    let _last_pid = bpm.new_page().expect("Failed to create a new page.");
    // No need to fetch the last page since we're not modifying it

    // Try to fetch pid0 again, expecting it to fail.
    let result = bpm.fetch_page(&pid0);
    assert!(
        result.is_none(),
        "Expected fetch_page for pid0 to return None."
    );
}

fn create_n_pages(bpm: &mut BufferPoolManager, n: usize) -> Vec<PageId> {
    (0..n)
        .map(|_| bpm.new_page().expect(NEW_PAGE_ERR_MSG))
        .collect()
}

/// Creates a new `PageId` object that is not present in `page_ids`. Note: to create a new page and
/// get the corresponding `PageId` of that page, use [`super::BufferPoolManager::new_page`] instead.
fn create_different_page_id(page_ids: &Vec<PageId>) -> PageId {
    page_ids.iter().max().unwrap_or(&0_u32) + 1
}

/// Sets the subset of `page_ids` that satisfy the criteria `criteria` to evictable, and returns a
/// list of those page ids whose corresponding page are now evictable.
fn set_pages_satisfying_criteria_to_evictable<F>(
    bpm: &mut BufferPoolManager,
    page_ids: &Vec<PageId>,
    criteria: F,
) -> Vec<PageId>
where
    F: Fn(&PageId) -> bool,
{
    page_ids
        .iter()
        .filter(|&page_id| criteria(page_id))
        .map(|page_id| {
            for _ in 0..bpm
                .get_pin_count(&page_id)
                .expect(NO_CORRESPONDING_PAGE_MSG)
            {
                bpm.unpin_page(page_id, false);
            }
            page_id.clone() // Assuming PageId implements Clone
        })
        .collect()
}

fn page_number_is_even(page_id: &PageId) -> bool {
    page_id % 2 == 0
}

fn new_disk_manager() -> Arc<RwLock<DiskManager>> {
    DiskManager::new_with_handle_for_test()
}

fn fetch_page_get_id(page_id: &PageId, bpm: &mut BufferPoolManager) -> PageId {
    *fetch_page(&page_id, bpm)
        .read()
        .expect(NO_CORRESPONDING_PAGE_MSG)
        .page_id()
}

fn fetch_page(page_id: &PageId, bpm: &mut BufferPoolManager) -> TablePageHandle {
    bpm.fetch_page(&page_id).expect(NO_CORRESPONDING_PAGE_MSG)
}

fn get_page_handle(
    buffer_pool_manager: &BufferPoolManager,
    page_id: &PageId,
) -> Option<TablePageHandle> {
    buffer_pool_manager
        .page_table
        .get(page_id)
        .map(|entry| Arc::clone(buffer_pool_manager.pages.get(*entry.frame_id()).unwrap()))
}

fn get_bpm_with_pool_size(pool_size: usize) -> BufferPoolManager {
    let disk_manager = new_disk_manager();
    BufferPoolManager::builder()
        .pool_size(pool_size)
        .replacer_k(5)
        .disk_manager(disk_manager)
        .build()
}

fn page_in_buffer(buffer_pool_manager: &BufferPoolManager, page_id: &PageId) -> bool {
    let frame_metadata = buffer_pool_manager.page_table.get(page_id);
    if frame_metadata.is_none() {
        return false;
    }
    let frame_id = frame_metadata.unwrap().frame_id();
    !buffer_pool_manager.free_list.contains(frame_id)
}

fn set_pages_to_dirty(bpm: &mut BufferPoolManager, page_ids: &Vec<PageId>) {
    page_ids
        .iter()
        .for_each(|page_id| bpm.set_is_dirty(page_id, true));
}
