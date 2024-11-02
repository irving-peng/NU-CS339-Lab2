use crate::common::{Error, Result};
use crate::storage::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::engine::Status;
use crate::storage::heap::{TableHeap, TableHeapIterator};
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use crate::storage::{engine, Engine, Key};
use crate::types::Table;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

pub struct HeapTableManager {
    heaps: HashMap<String, TableHeap>,
    bpm: Arc<RwLock<BufferPoolManager>>,
    key_directory: KeyDirectory,
}

impl HeapTableManager {
    pub fn new(bpm: &Arc<RwLock<BufferPoolManager>>) -> Self {
        Self {
            heaps: HashMap::new(),
            bpm: Arc::clone(bpm),
            key_directory: HashMap::new(),
        }
    }
}

/// Maps table name -> [ Map: bytestream key -> RecordId ]
pub type KeyDirectory = HashMap<String, BTreeMap<Vec<u8>, RecordId>>;

impl Engine for HeapTableManager {
    type ScanIterator<'a> = ScanIterator<'a>
    where
        Self: Sized + 'a;

    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.key_directory.contains_key(table.name()) {
            return Result::from(Error::InvalidInput(
                "Attempted to insert table that already exists!".to_string(),
            ));
        }
        self.key_directory
            .insert(table.name().to_string(), BTreeMap::new());
        self.heaps
            .insert(table.name().to_string(), TableHeap::new(table, &self.bpm));
        Ok(())
    }

    fn delete_table(&mut self, table_name: &str) -> Result<bool> {
        if !self.key_directory.contains_key(table_name) {
            return Ok(false);
        }
        self.key_directory.remove(table_name);
        self.heaps.remove(table_name);
        Ok(true)
    }

    fn get_table(&mut self, table_name: &str) -> Result<Option<Table>> {
        match self.heaps.get(table_name) {
            Some(heap) => Ok(Some(heap.schema())),
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        let heap = self
            .heaps
            .get_mut(key.table_name)
            .ok_or_else(|| Error::InvalidData(key.table_name.to_string()))?;
        heap.delete_tuple(key.record_id)
    }

    fn get(&mut self, key: Key) -> Result<Tuple> {
        let heap = self
            .heaps
            .get(key.table_name)
            .ok_or_else(|| Error::InvalidData(key.table_name.to_string()))?;
        heap.get_tuple(key.record_id)
    }

    fn insert(&mut self, table_name: &str, value: Tuple) -> Result<RecordId> {
        let heap = self
            .heaps
            .get_mut(table_name)
            .ok_or_else(|| Error::InvalidData(table_name.to_string()))?;
        heap.insert_tuple(value)
    }

    fn scan(&mut self, table_name: &str) -> Self::ScanIterator<'_>
    where
        Self: Sized,
    {
        let heap = self
            .heaps
            .get_mut(table_name)
            .unwrap_or_else(|| panic!("Could not access table {table_name}"));
        ScanIterator { inner: heap.iter() }
    }

    fn scan_dyn(&mut self) -> Box<dyn engine::ScanIterator + '_> {
        todo!()
    }

    fn update(&mut self, key: Key, value: Tuple) -> Result<()> {
        let heap = self
            .heaps
            .get_mut(key.table_name)
            .ok_or_else(|| Error::InvalidData(key.table_name.to_string()))?;
        heap.update_tuple(key.record_id, value)
    }

    fn status(&mut self) -> Result<Status> {
        todo!()
    }
}

pub struct ScanIterator<'a> {
    inner: TableHeapIterator<'a>,
}

impl Iterator for ScanIterator<'_> {
    type Item = Result<(RecordId, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}
