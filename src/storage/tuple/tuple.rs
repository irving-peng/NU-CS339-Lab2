use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Tuple {
    pub data: Vec<u8>,
}

impl From<Vec<u8>> for Tuple {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl From<&[u8]> for Tuple {
    fn from(v: &[u8]) -> Self {
        Self { data: v.to_vec() }
    }
}
