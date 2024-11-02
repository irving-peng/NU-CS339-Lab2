use crate::common::constants::{
    COULD_NOT_UNWRAP_BPM_MSG, INVALID_PID, NEW_PAGE_ERR_MSG, TUPLE_DOESNT_FIT_MSG,
};
use crate::common::{Error, Result};
use crate::storage::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::disk::disk_manager::PageId;
use crate::storage::page::{Page, RecordId, TablePage, TablePageHandle, TablePageIterator};
use crate::storage::tuple::{Tuple, TupleMetadata};
use crate::types::Table;
use std::sync::{Arc, RwLock};

/// Represents a table stored on disk.
#[derive(Debug)]
pub struct TableHeap {
    pub(crate) page_cnt: u32,
    pub(crate) schema: Table,
    // reference to the buffer pool manager instance shared between heap files
    pub(crate) buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    pub(crate) first_page_id: PageId,
    pub(crate) last_page_id: PageId,
}

impl TableHeap {
    pub fn new(schema: Table, bpm: &Arc<RwLock<BufferPoolManager>>) -> TableHeap {
        let bpm = Arc::clone(&bpm);
        let first_page_id = bpm.write().unwrap().new_page().unwrap().clone();

        TableHeap {
            page_cnt: 1,
            schema,
            buffer_pool_manager: bpm,
            first_page_id,
            last_page_id: first_page_id,
        }
    }

    pub fn schema(&self) -> Table {
        self.schema.clone()
    }

    pub fn num_pages(&self) -> u32 {
        self.page_cnt
    }

    /// creates a new page and updates corresponding heap metadata.
    pub fn create_new_page(&mut self) -> Result<PageId> {
        let binding = Arc::clone(&self.buffer_pool_manager);
        let mut bpm = binding.write().expect(COULD_NOT_UNWRAP_BPM_MSG);

        let new_page_id = match bpm.new_page() {
            Some(id) => id,
            None => return Err(Error::CreationError),
        };

        if let Some(page_handle) = bpm.fetch_page(&self.last_page_id) {
            page_handle.write().unwrap().set_next_page_id(new_page_id);
            self.last_page_id = new_page_id;
            self.page_cnt += 1;
            Ok(new_page_id)
        } else {
            Err(Error::CreationError)
        }
    }

    /// Fetches the tuple payload corresponding to the given record ID from the table heap.
    pub fn delete_tuple(&self, rid: &RecordId) -> Result<()> {
        let page = self.fetch_page_handle(&rid.page_id());
        let mut page_guard = page.write()?;

        page_guard.update_tuple_metadata(&TupleMetadata::deleted_payload_metadata(), rid)
    }

    pub fn get_tuple(&self, rid: &RecordId) -> Result<Tuple> {
        let page = self.fetch_page_handle(&rid.page_id());
        let page_guard = page.read()?;
        page_guard.get_tuple(rid)
    }

    pub fn insert_tuple(&mut self, tuple: Tuple) -> Result<RecordId> {
        let _ = self.get_page_slot(&tuple).unwrap_or_else(|| {
            // tuple payload won't fit in the existing page, make a new page
            self.create_new_page().expect(NEW_PAGE_ERR_MSG);
            self.get_page_slot(&tuple).expect(TUPLE_DOESNT_FIT_MSG)
        });

        let page = self.fetch_page_handle(&self.last_page_id);
        let mut page_guard = page.write().unwrap();
        let metadata = TupleMetadata::new(false);

        let slot_id = page_guard
            .insert_tuple(metadata, tuple)
            .expect(TUPLE_DOESNT_FIT_MSG);
        Ok(RecordId::new(self.last_page_id, slot_id))
    }

    pub fn update_tuple(&self, rid: &RecordId, payload: Tuple) -> Result<()> {
        let page_id = rid.page_id();

        let page = self.fetch_page_handle(&page_id);
        let mut page_guard = page.write().unwrap();
        let metadata = page_guard.get_tuple_metadata(rid)?;

        // If the tuple has a variable length field and the size of the updated tuple is different
        // from the existing tuple, delete the existing tuple and insert the new tuple.
        let existing_size = page_guard.get_tuple(rid)?.data.len();
        match existing_size == payload.data.len() {
            true => page_guard.update_tuple_in_place_unchecked(metadata, payload, rid),
            false => {
                page_guard
                    .update_tuple_metadata(&TupleMetadata::deleted_payload_metadata(), rid)?;
                page_guard.insert_tuple(TupleMetadata::new(false), payload);
                Ok(())
            }
        }
    }

    pub fn iter(&self) -> TableHeapIterator {
        let current_page_id = self.first_page_id;
        let current_page_iterator = TablePage::iter(self.fetch_page_handle(&current_page_id));

        TableHeapIterator {
            heap_file: self,
            current_page_id,
            current_page_iterator,
        }
    }

    pub(crate) fn fetch_page_handle(&self, page_id: &PageId) -> TablePageHandle {
        let mut bpm = self
            .buffer_pool_manager
            .write()
            .expect(COULD_NOT_UNWRAP_BPM_MSG);
        bpm.fetch_page(page_id).unwrap()
    }

    pub(crate) fn get_page_slot(&self, payload: &Tuple) -> Option<u16> {
        let page = self.fetch_page_handle(&self.last_page_id);
        let offset = page.read().unwrap().get_next_tuple_offset(payload);
        offset
    }
}

/// Iterator that sequentially iterates over all the tuples in a heap file.
/// It does not outlive the lifetime of its underlying heap file.
pub struct TableHeapIterator<'a> {
    heap_file: &'a TableHeap,
    current_page_id: PageId,
    current_page_iterator: TablePageIterator,
}

impl Iterator for TableHeapIterator<'_> {
    type Item = (RecordId, Tuple);

    /// Returns `Some(tuple)` if a tuple exists at the iterator's current slot in the page, and
    /// `None` if the iterator is at the end of the page and there aren't anymore tuples.
    fn next(&mut self) -> Option<Self::Item> {
        while self.current_page_id <= self.heap_file.last_page_id {
            // our page iterator produced a valid tuple!
            if let Some(item) = self.current_page_iterator.next() {
                return Some(item);
            }
            let next_page_id = self.current_page_iterator.next_page_id();
            match next_page_id {
                // that was the last page in the heap file
                INVALID_PID => break,
                // or, there's another page to iterate through!
                _ => {
                    self.current_page_id = next_page_id;
                    self.current_page_iterator =
                        TablePage::iter(self.heap_file.fetch_page_handle(&next_page_id));
                }
            }
        }
        None
    }
}
