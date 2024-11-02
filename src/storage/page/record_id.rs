use crate::common::constants::{INVALID_PID, INVALID_RID_MSG, INVALID_SLOT_ID_MSG};
use crate::common::{Error, Result};
use crate::storage::disk::disk_manager::PageId;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct RecordId {
    page_id: PageId,
    slot_id: u16,
}

// for heap-allocated objects within operators
pub const INVALID_RID: RecordId = RecordId {
    page_id: INVALID_PID,
    slot_id: 0,
};

impl RecordId {
    pub fn new(page_id: PageId, sid: u16) -> RecordId {
        RecordId {
            page_id,
            slot_id: sid,
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}:{}", self.page_id, self.slot_id)
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn slot_id(&self) -> u16 {
        self.slot_id
    }

    pub fn invalid_rid_message(record_id: &RecordId) -> String {
        format!("{} {:?}", INVALID_RID_MSG, record_id)
    }
    pub fn invalid_slot_id_message(slot_id: u16) -> String {
        format!("{} {}", INVALID_SLOT_ID_MSG, slot_id)
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < size_of::<PageId>() + size_of::<u16>() {
            return Err(Error::InvalidData("Byte slice is too short".to_string()));
        }

        // Deserialize `page_id`
        let page_id_bytes: &[u8] = &bytes[..size_of::<PageId>()];
        let page_id: PageId = bincode::deserialize(page_id_bytes)
            .map_err(|e| Error::InvalidData(format!("Failed to deserialize PageId: {}", e)))?;

        // Deserialize `slot_id`
        let slot_id_bytes: &[u8] = &bytes[size_of::<PageId>()..];
        let slot_id = u16::from_be_bytes(
            slot_id_bytes
                .try_into()
                .map_err(|_| Error::InvalidData("Failed to convert bytes to u16".to_string()))?,
        );

        Ok(RecordId { page_id, slot_id })
    }

    /// Serializes the RecordId to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // Serialize the PageId
        let page_id_bytes = bincode::serialize(&self.page_id)
            .map_err(|e| Error::InvalidData(format!("Failed to serialize PageId: {}", e)))?;

        // Serialize the slot_id
        let slot_id_bytes = self.slot_id.to_be_bytes().to_vec();

        // Combine both byte vectors
        let mut bytes = Vec::with_capacity(page_id_bytes.len() + slot_id_bytes.len());
        bytes.extend(page_id_bytes);
        bytes.extend(slot_id_bytes);

        Ok(bytes)
    }
}

impl PartialEq<Self> for RecordId {
    fn eq(&self, other: &Self) -> bool {
        self.page_id == other.page_id && self.slot_id == other.slot_id
    }
}

impl Eq for RecordId {} // implement Eq trait for RecordId, uses PartialEq

impl Ord for RecordId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.page_id == other.page_id {
            if self.slot_id < other.slot_id {
                return std::cmp::Ordering::Less;
            } else if self.slot_id > other.slot_id {
                return std::cmp::Ordering::Greater;
            } else {
                return std::cmp::Ordering::Equal;
            }
        } else if self.page_id < other.page_id {
            return std::cmp::Ordering::Less;
        } else {
            return std::cmp::Ordering::Greater;
        }
    }
}

impl PartialOrd for RecordId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod record_id_tests {
    use super::*;

    fn setup() -> RecordId {
        RecordId::new(1, 1)
    }

    #[test]
    fn test_page_id() {
        let rid = setup();
        assert_eq!(1, rid.page_id());
    }

    #[test]
    fn test_slot_id() {
        let rid = setup();
        assert_eq!(1, rid.slot_id);
    }

    #[test]
    fn test_to_string() {
        let rid = setup();
        assert_eq!("1:1", rid.to_string());
    }

    #[test]
    fn test_equals() {
        let rid1 = RecordId::new(1, 1);
        let rid1_copy = RecordId::new(1, 1);
        let rid2 = RecordId::new(2, 2);

        assert_eq!(rid1, rid1);
        assert_eq!(rid1, rid1_copy);
        assert_eq!(rid1_copy, rid1);
        assert_eq!(rid2, rid2);

        assert_ne!(rid1, rid2);
        assert_ne!(rid1_copy, rid2);
        assert_ne!(rid2, rid1);
        assert_ne!(rid2, rid1_copy);
    }

    #[test]
    fn test_comparison() {
        let rid1 = RecordId::new(1, 1);
        let rid2 = RecordId::new(2, 2);
        let rid3 = RecordId::new(3, 1);
        let rid4 = RecordId::new(4, 1);
        let rid5 = RecordId::new(5, 2);

        assert!(rid1 < rid2);
        assert!(rid2 < rid3);
        assert!(rid3 < rid4);
        assert!(rid4 < rid5);
    }
}
