use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;

// TODO(eyoon): make this lol
#[derive(Debug)]
pub struct TableIndex {}

impl TableIndex {
    pub fn new() -> Self {
        Self {}
    }
    pub fn iter(&self) -> TableIndexIterator {
        todo!();
    }
}

pub struct TableIndexIterator {}

impl Iterator for TableIndexIterator {
    type Item = (RecordId, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
