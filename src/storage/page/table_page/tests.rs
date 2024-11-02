use super::*;
use crate::common::utility::{
    create_random_full_page, create_random_row, create_table_definition_mixed_fields,
};
use crate::config::config::RUSTY_DB_PAGE_SIZE_BYTES;
use crate::storage::page::record_id::RecordId;
use crate::storage::page::Page;
use crate::storage::tuple::{Tuple, TupleMetadata};
use crate::types::{DataType, Table};
use std::sync::{Arc, RwLock};

#[test]
pub fn test_insert_tuple() {
    let mut page = TablePage::builder().page_id(0).next_page_id(1).build();
    let tuple = Tuple::from(vec![1_u8, 2_u8, 3_u8, 4_u8]); // as if a single int32 field
    let meta = TupleMetadata::new(false);
    let slot = page.insert_tuple(meta, tuple.clone()).unwrap();

    assert_eq!(1, page.tuple_count());
    assert_eq!(0, page.deleted_tuple_count());
    assert_eq!(1, page.get_next_page_id());

    let rid = RecordId::new(page.page_id, slot);
    assert_eq!(tuple, page.get_tuple(&rid).unwrap());
}

#[test]
pub fn test_overfull_page() {
    let schema = Table::builder()
        .name("test_table")
        .column("column0", DataType::Text, false, None, Some(130))
        .column("column1", DataType::Int, false, None, None)
        .column("column2", DataType::Bool, false, None, None)
        .column("column3", DataType::Float, false, None, None)
        .build_with_handle();

    let mut page = TablePage::builder().page_id(0).build();
    // cost of next_page_id (u32) + tuple_cnt (u16) + deleted_tuple_cnt (u16) = 8 bytes.
    let mut page_size: usize = 8;

    loop {
        let tuple = create_random_row(&schema, None).to_tuple(&schema).unwrap();
        let tuple_size = tuple.data.len();

        // Adding tuple would make page overfull.
        if page_size + tuple_size + 4 > RUSTY_DB_PAGE_SIZE_BYTES {
            assert!(page.get_next_tuple_offset(&tuple).is_none());
            break;
        }
        page.insert_tuple(TupleMetadata::new(false), tuple);
        // 4 bytes for tuple metadata.
        page_size += tuple_size + 4;
    }
}

#[test]
pub fn test_iterate_page() {
    let schema = Arc::new(create_table_definition_mixed_fields(3));
    let page = Arc::new(RwLock::new(create_random_full_page(&schema, None)));
    let iter = TablePage::iter(Arc::clone(&page));

    let page_guard = page.read().unwrap();
    assert_eq!(iter.count(), page_guard.tuple_count() as usize);
}
