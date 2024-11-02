use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Deserialize, Serialize)]
pub struct TupleMetadata {
    is_deleted: bool,
}

impl TupleMetadata {
    pub fn new(is_deleted: bool) -> Self {
        Self { is_deleted }
    }

    pub fn deleted_payload_metadata() -> TupleMetadata {
        Self::new(true)
    }

    pub fn set_deleted(&mut self, d: bool) {
        self.is_deleted = d;
    }

    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn to_string(&self) -> String {
        format!("Deleted: {})", self.is_deleted)
    }
}
