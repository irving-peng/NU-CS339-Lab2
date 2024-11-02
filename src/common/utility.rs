use crate::config::config::RUSTY_DB_PAGE_SIZE_BYTES;
use crate::storage::heap::TableHeap;
use crate::storage::page::{Page, RecordId, TablePage};
use crate::storage::tuple::{Row, TupleMetadata};
use crate::types::field::Field;
use crate::types::{Column, DataType, Table};
use rand::{random, Rng};
use rand_chacha::ChaCha8Rng;
use rand_core::SeedableRng;
use std::sync::Arc;

pub fn create_n_rows(
    n: usize,
    heap_file: &mut TableHeap,
    table_schema: &Arc<Table>,
) -> Vec<(RecordId, Row)> {
    (0..n)
        .map(|_| create_random_row(&table_schema, None))
        .map(|row| {
            (
                heap_file
                    .insert_tuple(row.to_tuple(&table_schema).unwrap())
                    .unwrap(),
                row,
            )
        })
        .collect()
}

pub fn create_random_column_definition(column_name: &String) -> Column {
    let mut rng = rand::thread_rng();
    let data_type_id = rng.gen_range(0..4);
    match data_type_id {
        0 => Column::builder()
            .name(column_name.to_string())
            .data_type(DataType::Bool)
            .build(),
        1 => Column::builder()
            .name(column_name.to_string())
            .data_type(DataType::Int)
            .build(),
        2 => Column::builder()
            .name(column_name.to_string())
            .data_type(DataType::Float)
            .build(),
        3 => {
            let size_bound = rng.gen_range(1..256);
            Column::builder()
                .name(column_name.to_string())
                .data_type(DataType::Text)
                .max_str_len(size_bound)
                .build()
        }
        _ => Column::builder()
            .name(column_name.to_string())
            .data_type(DataType::Invalid)
            .build(),
    }
}

pub fn create_random_fields(schema: &Table, seed_in: Option<u64>) -> Vec<Field> {
    let mut fields = vec![Field::new(DataType::Invalid); schema.col_count()];
    let mut seed = random();

    if seed_in.is_some() {
        seed = seed_in.unwrap();
    }

    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    for i in 0..schema.col_count() {
        match schema.get_column(i).get_data_type() {
            DataType::Bool => {
                let b = rng.gen_range(0..2);
                fields[i] = Field::from(b == 1);
            }
            DataType::Int => {
                let i_field = rng.gen_range(0..1000);
                fields[i] = Field::from(i_field);
            }
            DataType::Float => {
                let f: f32 = rng.gen_range(0.0..100000.0);
                fields[i] = Field::from(f);
            }
            DataType::Text => {
                let size = schema.get_column(i).get_max_str_len();
                let len = rng.gen_range(0..size);
                let mut s = String::new();
                for _j in 0..len {
                    s.push(rng.gen_range(33..123) as u8 as char); // limiting it to printable chars
                }
                fields[i] = Field::from(s);
            }
            _ => {
                panic!("Unsupported data type");
            }
        }
    }

    fields
}

/// Create a row for the given page schema
pub fn create_random_row(schema: &Arc<Table>, seed: Option<u64>) -> Row {
    Row::from(create_random_fields(schema, seed))
}

pub fn create_random_full_page(schema: &Arc<Table>, seed: Option<u64>) -> TablePage {
    let mut page = TablePage::builder().page_id(0).build();
    let mut payload_size: usize = 8; // cost of next_page_id, tuple_cnt, deleted_tuple_cnt
    let mut local_seed = random();
    if seed.is_some() {
        local_seed = seed.unwrap();
    }

    loop {
        // create a tuple, serialize it into a byte stream, and store on the page
        let row = create_random_row(schema, Some(local_seed));
        let tuple = row.to_tuple(schema).unwrap();

        let tuple_byte_size = tuple.data.len();
        if payload_size + tuple_byte_size + 4 > RUSTY_DB_PAGE_SIZE_BYTES {
            break;
        }
        page.insert_tuple(TupleMetadata::new(false), tuple);
        payload_size += tuple_byte_size + 4; // 4 bytes for tuple metadata;

        // Make each tuple different
        local_seed += 1;
    }

    page
}

pub fn create_random_page_n_tuples(schema: &Arc<Table>, n: usize, seed: Option<u64>) -> TablePage {
    let mut page = TablePage::builder().page_id(0).build();
    let mut local_seed = random();
    if seed.is_some() {
        local_seed = seed.unwrap();
    }

    for _i in 0..n {
        let tuple = create_random_row(schema, Some(local_seed))
            .to_tuple(&schema)
            .unwrap();

        local_seed += 1; // makes each tuple different
        if page.get_next_tuple_offset(&tuple).is_none() {
            // out of space
            break;
        } else {
            let meta = TupleMetadata::new(false);
            page.insert_tuple(meta, tuple);
        }
    }

    page
}

pub fn create_table_definition(num_columns: usize, table_name: &str) -> Table {
    let mut table = Table::new(&table_name);
    (0..num_columns).for_each(|i| {
        let column_name = format!("{}{}", table_name, i);
        table.add_column(
            &Column::builder()
                .name(column_name.to_string())
                .data_type(DataType::Int)
                .build(),
        );
    });
    table
}

pub fn create_table_definition_by_data_type(count: usize, data_type: DataType) -> Table {
    let mut table = Table::new("test_table");
    let mut rng = rand::thread_rng();

    match data_type {
        DataType::Text => {
            let columns: Vec<Column> = (0..count)
                .map(|i| {
                    let column_name = format!("column{}", i);
                    let size_bound = rng.gen_range(1..256);
                    Column::builder()
                        .name(column_name.to_string())
                        .data_type(DataType::Text)
                        .max_str_len(size_bound)
                        .build()
                })
                .collect();

            table.with_columns(columns);
        }
        _ => {
            let columns: Vec<Column> = (0..count)
                .map(|i| {
                    let column_name = format!("column{}", i);
                    Column::builder()
                        .name(column_name.to_string())
                        .data_type(data_type.clone())
                        .build()
                })
                .collect();

            table.with_columns(columns);
        }
    }
    table
}

pub fn create_table_definition_mixed_fields(count: usize) -> Table {
    let mut table = Table::new("test_table");
    let mut rng = rand::thread_rng();

    for i in 0..count {
        let column_name = format!("column{}", i);
        let data_type_id = rng.gen_range(0..4);
        match data_type_id {
            0 => {
                table.add_column(
                    &Column::builder()
                        .name(column_name.to_string())
                        .data_type(DataType::Bool)
                        .build(),
                );
            }
            1 => {
                table.add_column(
                    &Column::builder()
                        .name(column_name.to_string())
                        .data_type(DataType::Int)
                        .build(),
                );
            }
            2 => {
                table.add_column(
                    &Column::builder()
                        .name(column_name.to_string())
                        .data_type(DataType::Float)
                        .build(),
                );
            }
            3 => {
                let size_bound = rng.gen_range(1..256);
                table.add_column(
                    &Column::builder()
                        .name(column_name.to_string())
                        .data_type(DataType::Text)
                        .max_str_len(size_bound)
                        .build(),
                );
            }
            _ => {}
        }
    }
    table
}
