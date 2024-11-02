use super::*;
use crate::common::utility::create_table_definition;
use crate::types::field::Field;
use std::sync::Arc;

#[test]
pub fn test_comparison() {
    let mut fields = vec![
        Field::from(1),
        Field::from(2), // this will get modified later.
        Field::from(3),
        Field::from(4), // this will get modified later.
        Field::from(5),
    ];

    // Tuples with identical fields should be equal.
    let row = Row::from(fields.clone());
    let row_eq = Row::from(fields.clone());
    assert_eq!(row, row_eq);

    // Tuples with some differing fields should not be equal.
    fields[2] = fields[2].clone() + Field::from(2);
    fields[4] = fields[4].clone() + Field::from(2);
    let row_ne = Row::from(fields);
    assert_ne!(row, row_ne);
}

#[test]
pub fn test_mixed_types() {
    let fields = vec![
        Field::from(1),
        Field::from("hello"),
        Field::from(3.14),
        Field::from("world"),
        Field::from("foo"),
        Field::from(true),
        Field::from(42),
    ];

    let row1 = Row::from(fields.clone());
    assert_eq!(row1.size(), fields.len());
    fields
        .iter()
        .enumerate()
        .for_each(|(i, field)| assert_eq!(row1.get_field(i).unwrap(), *field));

    let fields2 = vec![
        Field::from(1),
        Field::from("wildcat"),
        Field::from(3.14),
        Field::from("welcome"),
        Field::from("bar"),
        Field::from(true),
        Field::from(42),
    ];

    let row2 = Row::from(fields2.clone());
    assert_eq!(row2.size(), fields2.len());
    assert_ne!(row1, row2);
}

#[test]
pub fn test_int_serialization() {
    let schema = Arc::new(create_table_definition(3, "test"));

    // Create a tuple and serialize it.
    let fields: Vec<Field> = (1..4).into_iter().map(|i| Field::from(i)).collect();
    let row = Row::from(fields.clone());

    // Create another row by deserializing the tuple from the initial row.
    let serialized = row.to_tuple(&schema).unwrap();
    let row2 = Row::from_tuple(serialized, &schema).unwrap();

    // The fields of both tuples should match.
    assert_eq!(row2.size(), 3);
    fields
        .iter()
        .enumerate()
        .for_each(|(i, field)| assert_eq!(row2.get_field(i).unwrap(), *field));
}
