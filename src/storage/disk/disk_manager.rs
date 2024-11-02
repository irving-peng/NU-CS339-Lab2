use crate::config::config::{RUSTY_DB_PAGE_SIZE_BYTES, RUST_DB_DATA_DIR};
use crate::storage::page::{Page, TablePage};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
#[cfg(test)]
use tempfile::NamedTempFile;

/// Offset into the database file
pub type PageId = u32;

#[derive(Debug)]
pub struct DiskManager {
    current_page_no: AtomicU32,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

impl DiskManager {
    /// Creates a new disk manager for the given database file `filename`, e.g. `example.db`
    pub fn new(filename: &str) -> Self {
        let path = Path::new(RUST_DB_DATA_DIR).join(filename);
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
            .expect("Unable to create or open file {path}.");
        let reader = file;
        let writer = reader.try_clone().expect("Unable to clone file {filename}");

        DiskManager {
            current_page_no: AtomicU32::new(0),
            writer: BufWriter::new(writer),
            reader: BufReader::new(reader),
        }
    }
    pub fn new_with_handle(filename: &str) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new(filename)))
    }

    pub fn allocate_new_page(&mut self) -> PageId {
        let page_id = self.increment_and_fetch_page_no();
        let new_page = TablePage::builder().page_id(page_id).build();

        self.write_page(new_page);
        page_id
    }

    /// No-op for now; a little out of scope for this project :)
    pub fn deallocate_page(&mut self, _page_id: &PageId) {
        // no-op
    }

    pub fn read_page(&mut self, page_id: &PageId) -> TablePage {
        let offset = Self::calculate_offset(page_id);
        self.reader
            .seek(SeekFrom::Start(offset as u64))
            .expect("Unable to access offset {offset}.");

        let mut buffer = [0; RUSTY_DB_PAGE_SIZE_BYTES];
        self.reader
            .read(&mut buffer[..])
            .expect("Unable to read page from disk.");

        TablePage::deserialize(&buffer)
    }

    pub fn write_page(&mut self, page: TablePage) {
        let page_id = page.page_id();
        let offset = Self::calculate_offset(page_id);
        let payload = page.serialize();

        self.writer
            .seek(SeekFrom::Start(offset as u64))
            .expect("Unable to access offset {offset}.");
        self.writer
            .write_all(&payload)
            .expect("Unable to write payload to offset {offset}.");
        self.writer
            .flush()
            .expect("Unable to flush buffer from write at offset {offset} to disk.");
    }

    fn calculate_offset(page_id: &PageId) -> u32 {
        page_id * RUSTY_DB_PAGE_SIZE_BYTES as u32
    }

    /// Increments the current value and returns the new value
    /// # Returns
    /// - `current_value` after the increment
    fn increment_and_fetch_page_no(&mut self) -> u32 {
        1 + self.current_page_no.fetch_add(1, Ordering::SeqCst)
    }

    #[cfg(test)]
    /// Disk Manager Constructor for testing using a temporary file.
    pub fn new_for_test() -> Self {
        let temp_file =
            NamedTempFile::new_in(RUST_DB_DATA_DIR).expect("Unable to create temp file");
        let writer = temp_file.reopen().expect("Unable to reopen temp file");

        DiskManager {
            current_page_no: AtomicU32::new(0),
            writer: BufWriter::new(writer),
            reader: BufReader::new(temp_file.into_file()),
        }
    }

    #[cfg(test)]
    /// Test-only version of `new_with_handle` that uses the test constructor.
    pub fn new_with_handle_for_test() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new_for_test()))
    }
}
