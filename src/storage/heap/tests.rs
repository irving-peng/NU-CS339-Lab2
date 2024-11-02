use crate::common::constants::NEW_PAGE_ERR_MSG;
use crate::common::{utility, Error, Result};
use crate::storage::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::heap::TableHeap;
use crate::storage::page::{Page, RecordId, TablePage, TablePageHandle};
use crate::storage::tuple::Row;
use crate::types::Table;
use rand::Rng;
use std::sync::{Arc, RwLock, RwLockReadGuard};

#[test]
fn test_heap_file_initialization() {
    let hf = create_random_heap_file();
    assert_eq!(1, hf.num_pages());
    assert_eq!(hf.first_page_id, hf.last_page_id);
}

#[test]
fn test_create_page() {
    // Note: Heap files have a page upon initialization, e.g. hf.num_pages() == 1
    let mut heap_file = create_random_heap_file();

    let new_page_id = heap_file.create_new_page().expect(NEW_PAGE_ERR_MSG);
    assert_ne!(heap_file.first_page_id, new_page_id);
    assert_eq!(heap_file.last_page_id, new_page_id);
    assert_eq!(heap_file.page_cnt, 2);
}

/// This test does NOT assume [`TableHeap::get_tuple`] works properly.
/// However, it does assume that [`super::TablePage::get_tuple`] functions as intended.
#[test]
fn test_insert_tuple() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let tuple = create_row(&table_schema);
    let rid = heap_file
        .insert_tuple(tuple.to_tuple(&table_schema).unwrap())
        .unwrap();

    let current_page = get_current_page_handle(&heap_file);
    let page_guard = current_page.read().unwrap();
    assert_eq!(0, page_guard.deleted_tuple_count());
    assert_eq!(1, page_guard.tuple_count());
    assert_eq!(
        tuple,
        get_tuple_from_page(&page_guard, &table_schema, &rid).unwrap()
    );
}

/// Like `test_insert_tuple`, this test does NOT assume [`TableHeap::get_tuple`] works properly.
#[test]
fn test_insert_many_tuples() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let rows: Vec<(RecordId, Row)> = utility::create_n_rows(
        25 * get_bpm_page_capacity(&heap_file),
        &mut heap_file,
        &table_schema,
    );
    rows.iter().for_each(|(rid, tuple)| {
        let page = heap_file.fetch_page_handle(&rid.page_id());
        let retrieved_tuple =
            get_tuple_from_page(&page.read().unwrap(), &table_schema, rid).unwrap();
        assert_eq!(*tuple, retrieved_tuple);
    })
}

/// This test assumes that [`TableHeap::insert_tuple`] works as intended.
#[test]
fn test_get_tuple() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let row = create_row(&table_schema);
    let rid = heap_file
        .insert_tuple(row.to_tuple(&table_schema).unwrap())
        .unwrap();
    assert_eq!(row, get_row(&heap_file, &table_schema, &rid).unwrap());
}

/// This test assumes that [`TableHeap::insert_tuple`] works as intended.
#[test]
fn test_get_many_tuples() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let rows: Vec<(RecordId, Row)> = utility::create_n_rows(
        25 * get_bpm_page_capacity(&heap_file),
        &mut heap_file,
        &table_schema,
    );
    rows.iter().for_each(|(rid, row)| {
        assert_eq!(*row, get_row(&heap_file, &table_schema, rid).unwrap());
    })
}

/// This test assumes that [`TableHeap::insert_tuple`] and [`TableHeap::get_tuple`] work as intended.
#[test]
fn test_update_tuple() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let tuple1 = create_row_with_seed(&table_schema, 1);
    let rid = heap_file
        .insert_tuple(tuple1.to_tuple(&table_schema).unwrap())
        .unwrap();
    assert_eq!(tuple1, get_row(&heap_file, &table_schema, &rid).unwrap());

    let tuple2 = create_row_with_seed(&table_schema, 2);
    assert_ne!(tuple1, tuple2);

    heap_file
        .update_tuple(&rid, tuple2.to_tuple(&table_schema).unwrap())
        .unwrap();
    let tuple = get_row(&heap_file, &table_schema, &rid).unwrap();
    assert_eq!(tuple2, tuple);
    assert_ne!(tuple1, tuple);
}

/// This test assumes that [`TableHeap::insert_tuple`] and [`TableHeap::get_tuple`] work as intended.
#[should_panic(expected = "")]
#[test]
fn test_delete_tuple() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let tuple = create_row(&table_schema);
    let rid = heap_file
        .insert_tuple(tuple.to_tuple(&table_schema).unwrap())
        .unwrap();
    assert_eq!(tuple, get_row(&heap_file, &table_schema, &rid).unwrap());

    heap_file.delete_tuple(&rid).unwrap();
    assert_eq!(
        Error::InvalidInput("".to_string()),
        get_row(&heap_file, &table_schema, &rid).expect_err("")
    );
}

/// This test assumes that [`TableHeap::insert_tuple`] and [`TableHeap::get_tuple`] work as intended.
#[test]
fn test_iter() {
    let mut heap_file = create_random_heap_file();
    let table_schema = Arc::new(heap_file.schema().clone());

    let rows: Vec<(RecordId, Row)> = utility::create_n_rows(
        25 * get_bpm_page_capacity(&heap_file),
        &mut heap_file,
        &table_schema,
    );
    let mut it = heap_file.iter();

    // Iterator should output tuples in sequential order...
    rows.iter().for_each(|(_rid, row)| {
        assert_eq!(
            Row::from_tuple(it.next().unwrap().1, &table_schema).unwrap(),
            *row
        )
    });

    // ...and should output `None` once there aren't any tuples left!
    assert!(it.next().is_none());
}

pub fn create_random_heap_file() -> TableHeap {
    let disk_manager = new_disk_manager();
    let bpm = Arc::new(RwLock::new(BufferPoolManager::new(50, 5, disk_manager)));
    let mut rng = rand::thread_rng();
    let schema = utility::create_table_definition(rng.gen_range(5..25), "test");

    TableHeap::new(schema, &bpm)
}

fn new_disk_manager() -> Arc<RwLock<DiskManager>> {
    DiskManager::new_with_handle_for_test()
}

pub fn create_row(table_schema: &Arc<Table>) -> Row {
    utility::create_random_row(table_schema, None)
}

pub fn create_row_with_seed(table_schema: &Arc<Table>, seed: u64) -> Row {
    utility::create_random_row(table_schema, Some(seed))
}

fn get_bpm_page_capacity(heap_file: &TableHeap) -> usize {
    heap_file.buffer_pool_manager.read().unwrap().size()
}

fn get_current_page_handle(heap_file: &TableHeap) -> TablePageHandle {
    Arc::clone(&heap_file.fetch_page_handle(&heap_file.last_page_id))
}

fn get_tuple_from_page(
    page_guard: &RwLockReadGuard<TablePage>,
    schema: &Table,
    rid: &RecordId,
) -> Result<Row> {
    Row::from_tuple(page_guard.get_tuple(rid)?, schema)
}

fn get_row(heap_file: &TableHeap, schema: &Table, rid: &RecordId) -> Result<Row> {
    Row::from_tuple(heap_file.get_tuple(rid)?, schema)
}
