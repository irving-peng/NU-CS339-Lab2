use crate::common::Result;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use crate::types::Table;
use serde::{Deserialize, Serialize};

pub struct Key<'a> {
    pub table_name: &'a str,
    pub record_id: &'a RecordId,
}

impl<'a> Key<'a> {
    pub fn new(table_name: &'a str, record_id: &'a RecordId) -> Key<'a> {
        Self {
            table_name,
            record_id,
        }
    }
}

/// A key/value storage engine.
///
/// Note(eyoon): a real key/value storage engine would have &[u8] byte streams as
/// both the input and output of all API calls. For the sake of pedantic clarity and
/// also because I ran out of time to properly implement it, this storage engine
/// trait will pass around (table name, record id) pairs instead of byte-stream keys.
pub trait Engine: Send {
    /// The iterator returned by scan()
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a;

    /// Creates a table.
    fn create_table(&mut self, table: Table) -> Result<()>;

    /// Deletes a table. Returns true if it exists and false otherwise.
    fn delete_table(&mut self, table_name: &str) -> Result<bool>;

    /// Gets a table with the given table name.
    fn get_table(&mut self, table_name: &str) -> Result<Option<Table>>;

    /// Deletes a key if one exists. Otherwise, does nothing.
    fn delete(&mut self, key: Key) -> Result<()>;

    /// Gets a value for a key if one exists.
    fn get(&mut self, key: Key) -> Result<Tuple>;

    /// Inserts a new tuple value into the table with name `table_name`,
    /// and returns the resultant record id for it.
    fn insert(&mut self, table_name: &str, value: Tuple) -> Result<RecordId>;

    /// Creates an iterator over the table's key/value pairs.
    fn scan(&mut self, table_name: &str) -> Self::ScanIterator<'_>
    where
        Self: Sized;

    /// Scan, but can be used from trait objects. This iterator uses
    /// dynamic dispatch, which incurs a runtime performance penalty.
    fn scan_dyn(&mut self) -> Box<dyn ScanIterator + '_>;

    /// Updates a tuple corresponding to the given record id with the provided value.
    fn update(&mut self, key: Key, value: Tuple) -> Result<()>;

    /// Returns engine status.
    fn status(&mut self) -> Result<Status>;
}

/// A scan iterator over a table
pub trait ScanIterator: Iterator<Item = Result<(RecordId, Tuple)>> {}
/// Blanket implementation of ScanIterator for any `I` satisfying the trait bound.
impl<I: Iterator<Item = Result<(RecordId, Tuple)>>> ScanIterator for I {}

/// Engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The name of the storage engine.
    pub name: String,
    /// The number of live keys in the engine.
    pub keys: u64,
    /// The logical size of live key/value pairs.
    pub size: u64,
}
