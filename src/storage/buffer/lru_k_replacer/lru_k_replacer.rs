use crate::storage::buffer::buffer_pool_manager::FrameId;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum AccessType {
    Unknown = 0,
    Lookup,
    Scan,
    Index,
}

#[derive(Debug)]
pub struct LRUKNode {
    /// History of last seen k timestamps of this page. Least recent timestamp stored in front.
    pub(crate) history: VecDeque<usize>,
    pub(crate) k: usize,
    pub(crate) is_evictable: bool,
}

impl LRUKNode {
    fn new(k: usize) -> Self {
        Self {
            history: VecDeque::with_capacity(k),
            k,
            is_evictable: false,
        }
    }

    /// # Returns
    /// - the k'th most recent timestamp's distance from the current timestamp if k accesses
    ///   have been recorded, and `usize::MAX` otherwise
    pub(crate) fn get_backwards_k_distance(&self, current_timestamp: usize) -> usize {
        if self.history.len() < self.k {
            return usize::MAX;
        }
        let k_th_last_access = self.history[self.history.len() - self.k];
        current_timestamp.saturating_sub(k_th_last_access)
    }

    pub(crate) fn has_infinite_backwards_k_distance(&self) -> bool {
        self.history.len() < self.k
    }
}

#[derive(Debug)]
pub struct LRUKReplacer {
    pub(crate) node_store: HashMap<FrameId, LRUKNode>,
    pub(crate) current_timestamp: usize,
    // Number of evictable frames in the replacer. Note: this might not be the size of `node_store`!
    pub(crate) curr_size: usize,
    // Maximum number of frames that can be stored in the replacer.
    pub(crate) max_size: usize,
    pub(crate) k: usize,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: num_frames,
            k,
        }
    }

    pub fn builder() -> LRUKReplacerBuilder {
        LRUKReplacerBuilder {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: None,
            k: None,
        }
    }

    /// Evict the frame with the largest backwards k-distance. If a frame has
    /// not been accessed k times, its backwards k-distance is considered to
    /// be infinite. If there are multiple frames with infinite k-distance,
    /// choose the one to evict based on LRU.
    ///
    /// # Returns
    /// - an Option that is either `Some(frame_id)` if a frame with id `frame_id` was evicted, and
    ///   `None` otherwise
    pub fn evict(&mut self) -> Option<FrameId> {
        let mut frame_to_evict: Option<FrameId> = None;
        let mut earliest_timestamp_with_infinity = usize::MAX; // Track earliest timestamp for infinite distances
        let mut max_k_distance = 0;
        for (&frame_id, node) in &self.node_store {
            if node.is_evictable {
                let k_distance = node.get_backwards_k_distance(self.current_timestamp);
                if node.has_infinite_backwards_k_distance() {
                    let first_access = *node.history.front().unwrap_or(&usize::MAX);
                    if frame_to_evict.is_none() || first_access < earliest_timestamp_with_infinity {
                        earliest_timestamp_with_infinity = first_access;
                        frame_to_evict = Some(frame_id);
                    }
                } else if frame_to_evict.is_none() || frame_to_evict.is_some() && earliest_timestamp_with_infinity == usize::MAX && k_distance > max_k_distance {
                    max_k_distance = k_distance;
                    frame_to_evict = Some(frame_id);
                }
            }
        }

        if let Some(evict_frame_id) = frame_to_evict {
            self.node_store.remove(&evict_frame_id);
            self.curr_size -= 1;
            return Some(evict_frame_id);
        }
        None
    }

    /// Record an access to a frame at the current timestamp.
    ///
    /// This method should update the k-history of the frame and increment the current timestamp.
    /// If the given `frame_id` is invalid (i.e. >= `max_size`), this method throws an exception.
    ///
    /// # Parameters
    /// - `frame_id`: The id of the frame that was accessed
    /// - `access_type`: The type of access that occurred (e.g., Lookup, Scan, Index)
    pub fn record_access(&mut self, frame_id: &FrameId, _access_type: AccessType) {
        // Validate frame_id
        if *frame_id >= self.max_size {
            panic!("Invalid frame_id: exceeds maximum size of the buffer pool.");
        }
        let node = self.node_store.entry(*frame_id)
            .or_insert_with(|| LRUKNode::new(self.k));
        // Update the access history
        if node.history.len() == self.k {
            node.history.pop_front(); // Remove the oldest timestamp if at max capacity
        }
        node.history.push_back(self.current_timestamp); // Add the current timestamp
        self.current_timestamp += 1;
    }

    /// Set the evictable status of a frame. Note that replacer's curr_size is equal
    /// to the number of evictable frames.
    ///
    /// If a frame was previously evictable and is set to be non-evictable,
    /// then curr_size should decrement. If a frame was previously non-evictable and
    /// is to be set to evictable, then curr_size should increment. If the frame id is
    /// invalid, throw an exception or abort the process.
    ///
    /// For other scenarios, this function should terminate without modifying anything.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame whose 'evictable' status will be modified
    /// - `set_evictable`: whether the given frame is evictable or not
    pub fn set_evictable(&mut self, frame_id: &FrameId, set_evictable: bool) {
        // Validate frame_id. Assuming `FrameId` is a `usize` or type alias for `usize`.
        if *frame_id >= self.max_size {
            panic!("Invalid frame_id: exceeds maximum size of the buffer pool.");
        }

        // Attempt to get a mutable reference to the node
        if let Some(node) = self.node_store.get_mut(frame_id) {
            // Only adjust `curr_size` if there is an actual transition
            if node.is_evictable != set_evictable {
                if set_evictable {
                    self.curr_size += 1;
                } else {
                    self.curr_size -= 1;
                }
                node.is_evictable = set_evictable; // Update the evictable status
            }
        }
    }

    /// Remove an evictable frame from the replacer, along with its access history.
    /// This function should also decrement replacer's size if removal is successful.
    ///
    /// Note that this is different from evicting a frame, which always removes the frame
    /// with the largest backward k-distance. This function removes the specified frame id,
    /// no matter what its backward k-distance is.
    ///
    /// If `remove` is called on a non-evictable frame, throw an exception or abort the
    /// process.
    ///
    /// If the specified frame is not found, directly return from this function.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame to be removed
    pub fn remove(&mut self, frame_id: &FrameId) {
        // Check if the frame exists in `node_store`
        if let Some(node) = self.node_store.get(frame_id) {
            // Ensure the frame is evictable before removing
            if !node.is_evictable {
                panic!("Attempted to remove a non-evictable frame");
            }
            // Remove the frame from `node_store` and decrement `curr_size`
            self.node_store.remove(frame_id);
            self.decrement_current_size();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_full_capacity(&self) -> bool {
        self.curr_size == self.max_size
    }
    pub fn size(&self) -> usize {
        self.curr_size
    }

    fn increment_current_size(&mut self) {
        self.curr_size += 1;
    }

    fn decrement_current_size(&mut self) {
        if self.curr_size == 0 {
            panic!("Attempted to decrement current size, which is already 0");
        }
        self.curr_size -= 1;
    }
}

pub struct LRUKReplacerBuilder {
    node_store: HashMap<FrameId, LRUKNode>,
    current_timestamp: usize,
    curr_size: usize,
    max_size: Option<usize>,
    k: Option<usize>,
}

impl LRUKReplacerBuilder {
    pub fn max_size(mut self, num_frames: usize) -> Self {
        assert!(num_frames > 0);
        self.max_size = Some(num_frames);
        self
    }

    pub fn k(mut self, k: usize) -> Self {
        assert!(k > 0);
        self.k = Some(k);
        self
    }

    pub fn build(self) -> LRUKReplacer {
        LRUKReplacer {
            node_store: self.node_store,
            current_timestamp: self.current_timestamp,
            curr_size: self.curr_size,
            max_size: self
                .max_size
                .expect("Replacer size was not specified before build."),
            k: self.k.expect("k was not specified before build."),
        }
    }
}
