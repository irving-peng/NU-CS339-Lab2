use crate::config::config::RUST_DB_DATA_DIR;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::{Page, RecordId, TablePage};
use crate::storage::tuple::{Tuple, TupleMetadata};
use std::sync::{Arc, RwLock};
use tempfile::NamedTempFile;

#[test]
fn test_write_and_read_page() {
    let disk_manager = new_disk_manager();

    let page_id = {
        let mut dm = disk_manager.write().unwrap();
        dm.allocate_new_page()
    };

    let mut page = TablePage::builder().page_id(page_id).build();
    let tuple_data = b"Hello, DiskManager!".to_vec();
    let tuple_metadata = TupleMetadata::new(false);
    let tuple = Tuple::from(&tuple_data[..]);

    let slot_id = page
        .insert_tuple(tuple_metadata, tuple.clone())
        .expect("Failed to insert tuple");
    let record_id = RecordId::new(page.page_id, slot_id);

    {
        let mut dm = disk_manager.write().unwrap();
        dm.write_page(page.clone());
    }

    let read_page = {
        let mut dm = disk_manager.write().unwrap();
        dm.read_page(&page_id)
    };

    let retrieved_tuple = read_page
        .get_tuple(&record_id)
        .expect("Failed to retrieve tuple");

    assert_eq!(
        retrieved_tuple, tuple,
        "Data read from disk does not match data written"
    );
}

/// Test that data persists across different instances of `DiskManager`.
#[test]
fn test_persistent_storage() {
    let page_id;
    let test_data = b"Persistent Data".to_vec();
    let tuple_metadata = TupleMetadata::new(false);
    let tuple = Tuple::from(&test_data[..]);

    // Create a temporary file within `rusty-db/data/` and get its filename.
    let temp_file = NamedTempFile::new_in(RUST_DB_DATA_DIR).expect("Failed to create temp file");
    let file_name = temp_file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // First `DiskManager` instance: write data.
    {
        let disk_manager = DiskManager::new_with_handle(&file_name);
        let mut dm = disk_manager.write().unwrap();
        page_id = dm.allocate_new_page();

        let mut page = TablePage::builder().page_id(page_id).build();

        page.insert_tuple(tuple_metadata, tuple.clone())
            .expect("Failed to insert tuple");

        dm.write_page(page.clone());
        // `DiskManager` goes out of scope and file is closed.
    }

    // Second `DiskManager` instance: read data.
    {
        let disk_manager = DiskManager::new_with_handle(&file_name);
        let read_page = {
            let mut dm = disk_manager.write().unwrap();
            dm.read_page(&page_id)
        };

        assert_eq!(
            read_page.tuple_info.len(),
            1,
            "Page {} should contain exactly one tuple",
            page_id
        );

        let record_id = RecordId::new(page_id, 0);
        let retrieved_tuple = read_page
            .get_tuple(&record_id)
            .expect("Failed to retrieve tuple");

        assert_eq!(
            retrieved_tuple, tuple,
            "Data read from disk does not match data written in previous instance"
        );
    }
}

/// Test writing and reading multiple pages to ensure each page maintains its own data.
#[test]
fn test_multiple_page_write_and_read() {
    let disk_manager = new_disk_manager();
    let num_pages = 5;
    let mut page_ids = Vec::new();

    // Test data and metadata.
    let tuple_metadata = TupleMetadata::new(false);

    // Allocate and write multiple pages.
    for _ in 0..num_pages {
        let page_id = {
            let mut dm = disk_manager.write().unwrap();
            dm.allocate_new_page()
        };
        page_ids.push(page_id);

        // Create a new TablePage.
        let mut page = TablePage::builder().page_id(page_id).build();

        // Prepare test data.
        let test_string = format!("Page number {}", page_id);
        let test_data = test_string.as_bytes().to_vec();
        let tuple = Tuple::from(&test_data[..]);

        page.insert_tuple(tuple_metadata.clone(), tuple.clone())
            .expect("Failed to insert tuple");

        // Write the updated page to disk.
        {
            let mut dm = disk_manager.write().unwrap();
            dm.write_page(page.clone())
        }
    }

    // Read back and verify each page.
    for &page_id in &page_ids {
        let read_page = {
            let mut dm = disk_manager.write().unwrap();
            dm.read_page(&page_id)
        };

        let record_id = RecordId::new(page_id, 0);

        let retrieved_tuple = read_page
            .get_tuple(&record_id)
            .expect(&format!("Failed to retrieve tuple from page {}", page_id));

        // Prepare expected data.
        let expected_string = format!("Page number {}", page_id);
        let expected_data = expected_string.as_bytes().to_vec();
        let expected_tuple = Tuple::from(&expected_data[..]);

        // Verify that the retrieved tuple matches the expected data
        assert_eq!(
            retrieved_tuple, expected_tuple,
            "Data read from page {} does not match expected data",
            page_id
        );
    }
}

fn new_disk_manager() -> Arc<RwLock<DiskManager>> {
    DiskManager::new_with_handle_for_test()
}
