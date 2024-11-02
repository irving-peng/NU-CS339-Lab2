use crate::common::{Error, Result};
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use crate::types::field::Field;
use crate::types::{DataType, Table};
use dyn_clone::DynClone;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::slice::Iter;

/// A row iterator.
pub type Rows = Box<dyn RowIterator>;

/// A Row iterator trait, which requires the iterator to be both clonable and
/// object-safe. Cloning is needed to be able to reset an iterator back to an
/// initial state, e.g. during nested loop joins. It has a blanket
/// implementation for all matching iterators.
pub trait RowIterator: Iterator<Item = Result<(RecordId, Row)>> + DynClone {}
impl<I: Iterator<Item = Result<(RecordId, Row)>> + DynClone> RowIterator for I {}
dyn_clone::clone_trait_object!(RowIterator);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Row {
    values: Vec<Field>,
}

impl From<Vec<Field>> for Row {
    fn from(v: Vec<Field>) -> Self {
        Row::new(v)
    }
}

impl From<Vec<&Field>> for Row {
    fn from(value: Vec<&Field>) -> Self {
        Row::new(value.into_iter().cloned().collect())
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.values.eq(&other.values)
    }
    fn ne(&self, other: &Self) -> bool {
        self.values.ne(&other.values)
    }
}

impl IntoIterator for Row {
    type Item = Field;
    type IntoIter = std::vec::IntoIter<Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl Row {
    fn new(values: Vec<Field>) -> Row {
        Row {
            values: values.to_vec(),
        }
    }

    pub fn iter(&self) -> Iter<Field> {
        self.values.iter()
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }

    pub fn get_field(&self, index: usize) -> Result<Field> {
        Ok(self
            .values
            .get(index)
            .ok_or_else(|| Error::OutOfBounds)?
            .clone())
    }

    pub fn update_field(&mut self, index: usize, new: Field) -> Result<()> {
        let field = self
            .values
            .get_mut(index)
            .ok_or_else(|| Error::OutOfBounds)?;

        match field.get_type() == new.get_type() {
            true => {
                *field = new;
                Ok(())
            }
            false => Result::from(Error::InvalidInput(new.to_string())),
        }
    }

    pub fn to_string(&self, str_len: Option<usize>) -> String {
        self.values
            .iter()
            .map(|field| match field.get_type() {
                DataType::Text => {
                    let mut text = field.to_string();
                    if let Some(len) = str_len {
                        text.truncate(len);
                    }
                    text
                }
                _ => field.to_string(),
            })
            .join(", ")
    }

    pub fn to_tuple(&self, schema: &Table) -> Result<Tuple> {
        Ok(Tuple::from(self.serialize(schema)?))
    }

    pub fn from_tuple(tuple: Tuple, schema: &Table) -> Result<Row> {
        Ok(Self::deserialize(tuple.data, schema))
    }

    /// Serializes the Row's header and data into a byte-stream, structured as follows:
    ///
    /// | variable length field offset map | field data in bytes |
    ///                 ^                               ^
    ///     a text field's `stored_offset` points       |
    ///     here, which stores the field's offset into here
    ///
    ///   a fixed length field's stored_offset is to the offset from the start of
    ///   the field data portion (possibly not the beginning of the byte stream!)
    pub fn serialize(&self, schema: &Table) -> Result<Vec<u8>> {
        let mut offsets = Vec::new(); // Offset map for variable-length fields
        let mut field_data = Vec::new(); // Field data portion

        let mut current_offset = 0;

        for (i, field) in self.values.iter().enumerate() {
            match field.get_type() {
                DataType::Text => {
                    let serialized_field = field.serialize();
                    let field_len = serialized_field.len();
                    offsets.push(current_offset as u16);
                    field_data.extend_from_slice(&serialized_field);
                    current_offset += field_len;
                },
                _ => {
                    let serialized_field = field.serialize();
                    field_data.extend_from_slice(&serialized_field);
                }
            }
        }
        let mut serialized_offsets = Vec::new();
        for offset in offsets {
            serialized_offsets.extend_from_slice(&offset.to_be_bytes());
        }
        serialized_offsets.extend_from_slice(&field_data);
        Ok(serialized_offsets)
    }

    /// Deserializes a byte stream into a Row object.
    ///
    /// `bytes` contains u16 offsets for variable-length fields, followed
    /// by fixed-length fields, with variable-length fields at the end.
    pub fn deserialize(bytes: Vec<u8>, schema: &Table) -> Self {
       // Get the offsets of the variable length text fields, if any exist.
        let variable_field_offsets: Vec<u16> = (0..schema.variable_length_fields())
            .map(|i| u16::from_be_bytes([bytes[2 * i], bytes[(2 * i) + 1]]))
            .collect();

        // The first byte in `bytes` of the field data
        let field_data_start = variable_field_offsets.len() * 2;

        let values = schema
            .columns()
            .iter()
            .map(|column| match column.get_data_type() {
                DataType::Text => {
                    // Get the index into the variable length field offset array.
                    let offset_index = column.stored_offset() as usize;
                    let start = *variable_field_offsets.get(offset_index).unwrap() as usize;
                    let end = if offset_index == variable_field_offsets.len() - 1 {
                        bytes.len()
                    } else {
                        *variable_field_offsets.get(offset_index + 1).unwrap() as usize
                    };

                    Field::deserialize(&bytes[start..end], DataType::Text)
                }
                datatype => {
                    // Get the offset of the field in the byte stream.
                    let start = column.stored_offset() as usize + field_data_start;
                    let end = start + column.length_bytes() as usize;

                    Field::deserialize(&bytes[start..end], datatype)
                }
            })
            .collect();
        Self { values }

    }
}
