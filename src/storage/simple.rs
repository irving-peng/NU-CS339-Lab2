use crate::common::Result;
use crate::storage::engine::Engine;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use crate::storage::Key;
use crate::types::Table;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A serial transactional key-value engine. It wraps an
/// underlying storage engine for raw key-value storage.
///
/// It does not execute any transactions concurrently.
pub struct Simple<E: Engine> {
    pub engine: Arc<Mutex<E>>,
}

impl<E: Engine> Simple<E> {
    /// Creates a new simple engine with the given storage engine.
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> Result<Transaction<E>> {
        Transaction::begin(self.engine.clone())
    }
}

/// A simple transaction
pub struct Transaction<E: Engine> {
    /// The underlying storage engine, shared by all transactions
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Transaction<E> {
    /// Begins a new transaction in read-write mode. Note that
    /// this will only get called once, as our simple engine
    /// runs serially without transactional concurrency.
    fn begin(engine: Arc<Mutex<E>>) -> Result<Self> {
        let session = engine.lock()?;
        // MVCC versioning bookkeeping stuff would get called here.
        drop(session);

        Ok(Self { engine })
    }

    /// Creates a tbale
    pub fn create_table(&self, table: Table) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.create_table(table)
    }

    /// Deletes a table.
    pub fn delete_table(&self, table_name: &str) -> Result<bool> {
        let mut engine = self.engine.lock()?;
        engine.delete_table(table_name)
    }

    /// Fetches a table
    pub fn fetch_table(&self, table_name: &str) -> Result<Option<Table>> {
        let mut engine = self.engine.lock()?;
        engine.get_table(table_name)
    }

    /// Deletes a key.
    pub fn delete(&self, key: Key) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.delete(key)
    }

    /// Fetches a key's value; returns `None` if it does not exist.
    pub fn get(&self, key: Key) -> Result<Tuple> {
        let mut engine = self.engine.lock()?;
        engine.get(key)
    }

    /// Inserts a tuple into the table with the given `table_name`.
    /// Returns the record id corresponding to the inserted tuple.
    pub fn insert(&self, table_name: &str, value: Tuple) -> Result<RecordId> {
        let mut engine = self.engine.lock()?;
        engine.insert(table_name, value)
    }

    /// Updates a key's value.
    pub fn update(&self, key: Key, value: Tuple) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.update(key, value)
    }

    /// Returns an iterator over the key/value items of the table.
    pub fn scan(&self, table: &str) -> ScanIterator<E> {
        ScanIterator::new(Arc::clone(&self.engine), table)
    }
}

// todo(eyoon): buffer the scaniterator
//
// /// An iterator over the latest live and visible key/value pairs for the txn.
// ///
// /// The (single-threaded) engine is protected by a mutex, and holding the mutex
// /// for the duration of the iteration can cause deadlocks (e.g. when the local
// /// SQL engine pulls from two tables concurrently during a join). Instead, we
// /// pull and buffer a batch of rows at a time, and release the mutex in between.
// ///
// /// This does not implement DoubleEndedIterator (reverse scans), since the SQL
// /// layer doesn't currently need it.
// #[allow(clippy::type_complexity)]
pub struct ScanIterator<E: Engine> {
    /// The engine.
    engine: Arc<Mutex<E>>,
    /// A buffer of live and visible key/value pairs to emit.
    buffer: VecDeque<(RecordId, Tuple)>,
    /// The name of the table this iterates over
    table: String,
    /// The position of the current tuple in the iterator
    i: usize,
}

/// Implement Clone manually. Deriving it requires Engine: Clone.
impl<E: Engine> Clone for ScanIterator<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            buffer: self.buffer.clone(),
            table: self.table.clone(),
            i: self.i,
        }
    }
}
//
impl<E: Engine> ScanIterator<E> {
    /// The number of live keys to pull from the engine at a time.
    #[cfg(not(test))]
    const BUFFER_SIZE: usize = 1000;
    /// Pull only 2 keys in tests, to exercise this more often.
    #[cfg(test)]
    const BUFFER_SIZE: usize = 4;

    /// Creates a new scan iterator.
    fn new(engine: Arc<Mutex<E>>, table: &str) -> Self {
        let buffer = VecDeque::with_capacity(Self::BUFFER_SIZE);
        Self {
            engine,
            buffer,
            table: table.to_string(),
            i: 0,
        }
    }

    /// Fills the buffer, if there's any pending items.
    fn fill_buffer(&mut self) -> Result<()> {
        // Check if there's anything to buffer.
        if self.buffer.len() >= Self::BUFFER_SIZE {
            return Ok(());
        }

        let mut engine = self.engine.lock()?;
        let mut iter = engine.scan(&self.table).peekable();
        // Iterator is exhausted; no more tuples to insert into the buffer.
        if iter.peek().into_iter().skip(self.i).next().is_none() {
            return Ok(());
        }
        // Skip to the current
        while let Some((rid, tuple)) = iter.next().transpose()? {
            self.buffer.push_back((rid, tuple));
            self.i += 1;
        }
        Ok(())
    }
}

impl<E: Engine> Iterator for ScanIterator<E> {
    type Item = Result<(RecordId, Tuple)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fill_buffer() {
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(Ok)
    }
}
