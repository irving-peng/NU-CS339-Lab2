// LRUkReplacer
pub const INF: usize = usize::MAX;

// DiskManager
pub const INVALID_PID: u32 = u32::MAX;

// BufferPoolManager
pub const NO_CORRESPONDING_FRAME_ID_MSG: &str =
    "No frame corresponding to page_id {page_id} exists in the page page.";
pub const NO_CORRESPONDING_PAGE_MSG: &str =
    "No page corresponding to page_id {page_id} exists in the buffer pool.";

// TableHeap
pub const COULD_NOT_UNWRAP_BPM_MSG: &str =
    "Could not unwrap buffer pool manager from RwLock instance";
pub const COULD_NOT_UNWRAP_SYSTEM_CATALOG_MSG: &str =
    "Could not unwrap buffer pool manager from RwLock instance";
pub const NO_PAGE_EXISTS_MSG: &str = "No page exists corresponding to {page_id}";
pub const NEW_PAGE_ERR_MSG: &str = "Could not get a new page from the buffer pool manager.";
pub const TUPLE_DOESNT_FIT_MSG: &str = "Tuple doesn't fit on the page.";

// RecordId
pub const INVALID_RID_MSG: &str = "Invalid record id.";
pub const INVALID_SLOT_ID_MSG: &str = "Invalid slot id.";
pub const DELETED_TUPLE_MSG: &str = "Tuple corresponding to given record id is deleted.";

// SystemCatalog
pub const NO_TABLE_FOUND_FOR_TID_MSG: &str = "No page corresponding to the given tid found.";
pub const NO_TABLE_INDEXES_FOUND_FOR_TID_MSG: &str =
    "No indexes corresponding to the given tid found.";
pub const NO_TID_FOUND_FOR_NAME_MSG: &str =
    "No TableId corresponding to the given page name found.";
