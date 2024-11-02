use crate::common::constants::INVALID_PID;
use crate::common::{Error, Result};
use crate::config::config::RUSTY_DB_PAGE_SIZE_BYTES;
use crate::storage::disk::disk_manager::PageId;
use crate::storage::page::record_id::RecordId;
use crate::storage::page::Page;
use crate::storage::tuple::{Tuple, TupleMetadata};
use std::mem;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

pub type TablePageHandle = Arc<RwLock<TablePage>>;

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct TupleInfo {
    pub(crate) offset: u16,
    pub(crate) size_bytes: u16,
    pub(crate) metadata: TupleMetadata,
}

#[derive(Clone, Debug)]
pub struct TablePage {
    pub(crate) page_id: PageId,
    // stored as a linked list of pages.
    pub(crate) next_page_id: PageId,
    pub(crate) data: Vec<u8>,
    // Number of non-deleted tuples.
    pub(crate) tuple_cnt: u16,
    // Number of deleted tuples.
    pub(crate) deleted_tuple_cnt: u16,
    pub(crate) tuple_info: Vec<TupleInfo>,
    pub is_dirty: bool,
}

impl TablePage {
    // page are in a linked list, use next_page_id to iterate through pages.
    fn new(page_id: PageId, next_page_id: PageId) -> TablePage {
        TablePage {
            page_id,
            next_page_id,
            data: vec![0; RUSTY_DB_PAGE_SIZE_BYTES],
            tuple_cnt: 0,
            deleted_tuple_cnt: 0,
            tuple_info: Vec::new(),
            is_dirty: false,
        }
    }
    pub fn builder() -> TablePageBuilder {
        TablePageBuilder::new()
    }

    pub fn get_next_page_id(&self) -> u32 {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, pid: u32) {
        self.next_page_id = pid;
    }

    /// Returns the total number of tuples (both deleted and non-deleted)
    /// on the page. Note that deleted tuples are not overwritten by new
    /// tuples, and are instead marked with gravestones by their metadata.
    fn total_tuple_count(&self) -> u16 {
        debug_assert_eq!(
            self.tuple_cnt + self.deleted_tuple_cnt,
            self.tuple_info.len() as u16
        );
        self.tuple_cnt + self.deleted_tuple_cnt
    }

    pub fn get_next_tuple_offset(&self, payload: &Tuple) -> Option<u16> {
        let tuple_size_bytes = payload.data.len();
        let tuples_end = match self.total_tuple_count() {
            0 => RUSTY_DB_PAGE_SIZE_BYTES,
            _ => self.tuple_info[(self.total_tuple_count() - 1) as usize].offset as usize,
        };
        if tuple_size_bytes > tuples_end {
            return None;
        }
        // tuples are positioned at the end of the page growing inward, with new tuples appended to
        // the front, e.g. | ... t_{n}, t_{n-1}, ... t_{0} |.
        let tuples_start = (tuples_end - tuple_size_bytes) as u16;
        let header_size = 8 + (self.total_tuple_count() + 1) * 4;

        // Recall that the header and tuples are positioned on opposite sides of the page, growing
        // inward toward each other, i.e. | header => free space <= tuples |.
        Some(tuples_start).filter(|_| header_size < tuples_start)
    }

    pub fn update_tuple_in_place_unchecked(
        &mut self,
        meta: TupleMetadata,
        tuple: Tuple,
        rid: &RecordId,
    ) -> Result<()> {
        let slot = rid.slot_id() as usize;
        if slot >= self.total_tuple_count() as usize {
            panic!("Invalid slot ID");
        }

        // only support updating tuple payloads of equal length.
        let len = self.tuple_info[slot].size_bytes as usize;
        assert_eq!(len, tuple.data.len());

        // Update both payload metadata.
        let old_meta = self.tuple_info[slot].metadata;
        self.update_tuple_cnt(&old_meta.is_deleted(), &meta.is_deleted());

        self.tuple_info[slot].metadata = meta;

        // Along with tuple data.
        let offset = self.tuple_info[slot].offset as usize;
        self.data[offset..(offset + len)].copy_from_slice(&tuple.data);

        Ok(())
    }

    pub fn update_tuple_cnt(&mut self, old_meta_delete: &bool, new_meta_delete: &bool) {
        match (old_meta_delete, new_meta_delete) {
            (true, false) => {
                // The tuple was deleted before but is now undeleted.
                self.tuple_cnt += 1;
                self.deleted_tuple_cnt -= 1;
            }
            (false, true) => {
                // The tuple was not deleted before but is now deleted.
                self.tuple_cnt -= 1;
                self.deleted_tuple_cnt += 1;
            }
            _ => {}
        }
    }

    // Returns an iterator over all Tuples on this page.
    pub fn iter(table_page: Arc<RwLock<Self>>) -> TablePageIterator {
        TablePageIterator {
            page: Arc::clone(&table_page),
            index: AtomicU16::new(0),
        }
    }

    pub fn create_invalid_page() -> TablePage {
        TablePage::new(INVALID_PID, INVALID_PID)
    }

    pub fn is_invalid(&self) -> bool {
        self.page_id == INVALID_PID && self.next_page_id == INVALID_PID
    }
}

impl Page for TablePage {
    type InsertOutputType = u16;
    type ConcretePageType = Self;

    fn get_tuple(&self, rid: &RecordId) -> Result<Tuple> {
        let slot = rid.slot_id() as usize;
        if slot >= self.total_tuple_count() as usize {
            return Err(Error::InvalidData("slot id out of bounds".into()));
        }
        let tuple_info = &self.tuple_info[slot];
        let tuple_data = &self.data[tuple_info.offset as usize..(tuple_info.offset + tuple_info.size_bytes) as usize];
        Ok(Tuple::from(tuple_data.to_vec()))
    }

    fn insert_tuple(
        &mut self,
        meta: TupleMetadata,
        tuple: Tuple,
    ) -> Option<Self::InsertOutputType> {
        let offset = self.get_next_tuple_offset(&tuple)?;
        let slot_id = self.total_tuple_count();
        self.tuple_info.push(TupleInfo {
            offset,
            size_bytes: tuple.data.len() as u16,
            metadata: meta,
        });
        let start = offset as usize;
        let end = start + tuple.data.len();
        self.data[start..end].copy_from_slice(&tuple.data);
        self.tuple_cnt += 1;
        self.is_dirty = true;
        Some(slot_id)
    }

    fn get_tuple_metadata(&self, rid: &RecordId) -> Result<TupleMetadata> {
        let slot = rid.slot_id() as usize;
        if slot >= self.total_tuple_count() as usize {
            return Err(Error::InvalidData("Slot ID out of bounds".into()));
        }
        Ok(self.tuple_info[slot].metadata)
    }

    fn update_tuple_metadata(&mut self, metadata: &TupleMetadata, rid: &RecordId) -> Result<()> {
        let slot = rid.slot_id() as usize;

        if slot >= self.total_tuple_count() as usize {
            return Err(Error::InvalidData("Slot ID out of bounds".into()));
        }
        let old_metadata = self.tuple_info[slot].metadata;
        self.update_tuple_cnt(&old_metadata.is_deleted(), &metadata.is_deleted());
        self.tuple_info[slot].metadata = *metadata;
        self.is_dirty = true;
        Ok(())
    }

    fn get_is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_is_dirty(&mut self, is_dirty: bool) -> bool {
        self.is_dirty = is_dirty;
        self.is_dirty
    }

    fn page_id(&self) -> &PageId {
        &self.page_id
    }

    fn tuple_count(&self) -> u16 {
        self.tuple_cnt
    }

    fn deleted_tuple_count(&self) -> u16 {
        self.deleted_tuple_cnt
    }

    /// Note: data: Vec<u8> remains serialized in the TablePage; serialization happens incrementally
    /// in [`Self::insert_tuple`]
    fn serialize(&self) -> Vec<u8> {
        // Copy out tuple contents.
        let mut result = self.data.clone();

        let mut cursor = 0;
        // page_id: PageId,
        let page_id_size = mem::size_of::<PageId>();
        let page_id_bytes = bincode::serialize(&self.page_id).unwrap();
        result[cursor..(cursor + page_id_size)].copy_from_slice(&page_id_bytes[..]);
        cursor += page_id_size;

        // next_page_id: u32
        let next_page_id_bytes = self.next_page_id.to_le_bytes();
        result[cursor..(cursor + 4)].copy_from_slice(&next_page_id_bytes);
        cursor += 4;

        // tuple_cnt: u16,
        let tuple_cnt_bytes = self.tuple_cnt.to_le_bytes();
        result[cursor..(cursor + 2)].copy_from_slice(&tuple_cnt_bytes);
        cursor += 2;

        // deleted_tuple_cnt: u16
        let deleted_tuple_cnt_bytes = self.deleted_tuple_cnt.to_le_bytes();
        result[cursor..(cursor + 2)].copy_from_slice(&deleted_tuple_cnt_bytes);
        cursor += 2;

        // tuple_info: Vec<TupleInfo>
        self.tuple_info.iter().for_each(|info| {
            match info.metadata.is_deleted() {
                true => {
                    // this slot is vacant
                    result[cursor..(cursor + 4)].fill(0);
                    cursor += 4;
                }
                false => {
                    let offset_bytes = info.offset.to_le_bytes();
                    result[cursor..(cursor + 2)].copy_from_slice(&offset_bytes);
                    cursor += 2;

                    let size_bytes = info.size_bytes.to_le_bytes();
                    result[cursor..(cursor + 2)].copy_from_slice(&size_bytes);
                    cursor += 2;
                }
            }
        });

        result
    }

    // deserialize buffer to self thereby reinitializing the page
    /// Note: data: Vec<u8> remains serialized in the TablePage; deserialization happens on-demand;
    ///       see [`crate::storage::tuple::row::get_field`]
    fn deserialize(buffer: &[u8]) -> Self::ConcretePageType {
        let mut page = TablePage::builder().page_id(0).build();
        page.data = buffer.to_vec();
        let mut cursor = 0;

        // page_id: PageId
        let page_id_size = mem::size_of::<PageId>();
        let page_id_bytes = &buffer[cursor..(cursor + page_id_size)];
        page.page_id = bincode::deserialize(&page_id_bytes).unwrap();
        cursor += page_id_size;

        // next_page_id: u32
        let next_page_id_bytes = buffer[cursor..(cursor + 4)].to_vec();
        page.next_page_id = u32::from_le_bytes(next_page_id_bytes.try_into().unwrap());
        cursor += 4;

        // tuple_cnt: u16
        let tuple_cnt_bytes = buffer[cursor..(cursor + 2)].to_vec();
        page.tuple_cnt = u16::from_le_bytes(tuple_cnt_bytes.try_into().unwrap());
        cursor += 2;

        // deleted_tuple_cnt: u16
        let deleted_tuple_cnt_bytes = buffer[cursor..(cursor + 2)].to_vec();
        page.deleted_tuple_cnt = u16::from_le_bytes(deleted_tuple_cnt_bytes.try_into().unwrap());
        cursor += 2;

        // tuple_info: Vec<TupleInfo>
        (0..(page.tuple_cnt + page.deleted_tuple_cnt)).for_each(|_| {
            let offset_bytes = buffer[cursor..(cursor + 2)].to_vec();
            let offset = u16::from_le_bytes(offset_bytes.try_into().unwrap());
            cursor += 2;

            let size_bytes = buffer[cursor..(cursor + 2)].to_vec();
            let size = u16::from_le_bytes(size_bytes.try_into().unwrap());
            cursor += 2;

            let mut deleted = false;
            if size == 0 && offset == 0 {
                deleted = true;
            }

            let meta = TupleMetadata::new(deleted);
            let tuple_info = TupleInfo {
                offset,
                size_bytes: size,
                metadata: meta,
            };
            page.tuple_info.push(tuple_info);
        });

        // tuple data: Vec<u8>
        let tuple_data = buffer[0..RUSTY_DB_PAGE_SIZE_BYTES].to_vec();
        page.data = tuple_data;

        page
    }
}

pub struct TablePageIterator {
    pub(crate) page: Arc<RwLock<TablePage>>,
    pub(crate) index: AtomicU16,
}

impl TablePageIterator {
    pub fn next_page_id(&self) -> PageId {
        self.page.read().unwrap().get_next_page_id()
    }

    /// Returns the next tuple payload on the table, if one exists.
    fn tuple_if_exists(
        &self,
        page_slot: u16,
        page_guard: &RwLockReadGuard<TablePage>,
    ) -> Option<(RecordId, Tuple)> {
        match page_guard.tuple_info[page_slot as usize]
            .metadata
            .is_deleted()
        {
            // tombstone tuple; no tuple to return.
            true => None,
            // tuple is not deleted; return it!
            false => {
                let rid = RecordId::new(page_guard.page_id, page_slot);
                page_guard
                    .get_tuple(&rid)
                    .map_or_else(|_| None, |payload| Some((rid, payload)))
            }
        }
    }
}

impl Iterator for TablePageIterator {
    type Item = (RecordId, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        let page_guard = self.page.read().unwrap();

        // Use a loop to skip deleted tuples and find the next valid one.
        loop {
            let page_slot = self.index.fetch_add(1, Ordering::SeqCst);
            if page_slot >= page_guard.total_tuple_count() {
                // No more valid tuples.
                return None;
            }
            // Return non-deleted tuple, if encountered.
            if let Some(item) = self.tuple_if_exists(page_slot, &page_guard) {
                return Some(item);
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.index.fetch_add(n as u16, Ordering::SeqCst);
        self.next()
    }
}

pub struct TablePageBuilder {
    page_id: Option<PageId>,
    next_page_id: Option<PageId>,
}

impl TablePageBuilder {
    fn new() -> TablePageBuilder {
        TablePageBuilder {
            page_id: None,
            next_page_id: None,
        }
    }

    pub fn page_id(&mut self, page_id: PageId) -> &mut Self {
        self.page_id = Some(page_id);
        self
    }
    pub fn next_page_id(&mut self, next_page_id: PageId) -> &mut Self {
        self.next_page_id = Some(next_page_id);
        self
    }
    pub fn build(&self) -> TablePage {
        TablePage::new(
            self.page_id
                .expect("Cannot build TablePage without a `page_id`."),
            self.next_page_id.unwrap_or(INVALID_PID),
        )
    }
}
