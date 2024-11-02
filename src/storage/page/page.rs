use crate::common::Result;
use crate::storage::disk::disk_manager::PageId;
use crate::storage::page::record_id::RecordId;
use crate::storage::tuple::{Tuple, TupleMetadata};

/// Stores serialized tuples (which we will refer to as "payloads" to avoid confusion) in memory.
pub trait Page {
    type InsertOutputType;
    type ConcretePageType;

    /// Retrieves a tuple identified by the given `rid` from the page.
    fn get_tuple(&self, rid: &RecordId) -> Result<Tuple>;

    /// Inserts a tuple with the given metadata into the page.
    fn insert_tuple(&mut self, meta: TupleMetadata, tuple: Tuple)
        -> Option<Self::InsertOutputType>;

    /// Obtains metadata associated with the tuple identified by the given `rid`.
    fn get_tuple_metadata(&self, rid: &RecordId) -> Result<TupleMetadata>;

    /// Updates metadata for the specific tuple identified by the given `rid`.
    fn update_tuple_metadata(&mut self, metadata: &TupleMetadata, rid: &RecordId) -> Result<()>;

    /// Returns if a page is dirty, i.e. has been modified since it was last written to disk, or not.
    fn get_is_dirty(&self) -> bool;

    /// Sets the page's dirty status to `is_dirty`, returning a boolean indicating if the dirty state
    /// changed.
    fn set_is_dirty(&mut self, is_dirty: bool) -> bool;

    /// Returns the unique identifier for the page. In this DBMS, it is the page's offset into the
    /// database file on disk (see [`crate::storage::disk::disk_manager`]).
    fn page_id(&self) -> &PageId;

    /// Returns the number of (non-deleted) tuple payloads currently stored in the page.
    fn tuple_count(&self) -> u16;

    /// Returns the number of deleted tuple payloads within the page.
    fn deleted_tuple_count(&self) -> u16;

    /// Serializes the current state of the page into a byte vector.
    fn serialize(&self) -> Vec<u8>;

    /// Deserializes the given byte slice into an instance of the concrete page type. It is expected
    /// that the buffer is in the correct format for the specific implementation.
    fn deserialize(buffer: &[u8]) -> Self::ConcretePageType;
}
