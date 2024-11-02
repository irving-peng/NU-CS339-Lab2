use super::AccessType;
use super::*;
use crate::common::constants::INF;
use crate::storage::buffer::buffer_pool_manager::FrameId;
use crate::storage::buffer::lru_k_replacer::lru_k_replacer::LRUKNode;
use rand::{random, Rng};

const DUMMY_ACCESS_TYPE: AccessType = AccessType::Lookup;

#[test]
fn test_replacer_fields() {
    let replacer_size = random::<usize>();
    let k = random::<usize>();
    let replacer = LRUKReplacer::builder().max_size(replacer_size).k(k).build();

    assert_eq!(replacer.k, k);
    assert_eq!(replacer.max_size, replacer_size);
    assert_eq!(replacer.node_store.capacity(), 0);
    assert_eq!(replacer.node_store.capacity(), replacer.curr_size);
}

#[test]
fn test_evict_basic() {
    let k = 3_usize;
    let mut replacer = LRUKReplacer::builder().max_size(10).k(k).build();

    // no frames to evict.
    assert_eq!(replacer.evict(), None);

    let oldest_fid = get_new_frame_and_record_access(&mut replacer);
    let fid2 = get_new_frame_and_record_access(&mut replacer);
    let fid3 = get_new_frame_and_record_access(&mut replacer);

    // none of the frames are set to evictable.
    assert_eq!(replacer.evict(), None);

    // all frames have infinite backwards k-distance; evict based on LRU.
    set_multiple_frames_evictable(&mut replacer, &vec![oldest_fid, fid2, fid3]);
    assert_eq!(replacer.evict().unwrap(), oldest_fid);

    // evict based on LRU even if one of the frames has non-infinite backwards k-distance.
    let fid4 = get_new_frame_and_record_access(&mut replacer);
    replacer.set_evictable(&fid4, true);
    record_access_frames_n_times(&mut replacer, &vec![fid2, fid3], k);
    assert!(!get_node(&replacer, &fid2).has_infinite_backwards_k_distance());
    assert!(!get_node(&replacer, &fid3).has_infinite_backwards_k_distance());
    assert_eq!(replacer.evict().unwrap(), fid4);

    // when evicting based on LRU-k, frame with the largest backwards k-distance is evicted.
    let fid2_k_distance = get_backwards_k_distance_for_node(&mut replacer, &fid2);
    let fid3_k_distance = get_backwards_k_distance_for_node(&mut replacer, &fid3);
    assert!(fid2_k_distance > fid3_k_distance);
    assert_eq!(replacer.evict().unwrap(), fid2);
}


#[test]
#[should_panic]
fn test_record_access_panics_for_invalid_frame_id() {
    let replacer_size = 5_usize;
    let mut replacer = LRUKReplacer::builder()
        .max_size(replacer_size)
        .k(100)
        .build();

    let invalid_frame_id = replacer_size as FrameId;
    replacer.record_access(&invalid_frame_id, AccessType::Lookup);
}

#[test]
fn test_record_access() {
    let mut replacer = LRUKReplacer::builder().max_size(10).k(5).build();
    let mut current_timestamp = replacer.current_timestamp;

    // record access on a new frame will add new node with curr timestamp and increment timestamp.
    let frame_id = get_new_frame_and_record_access(&mut replacer);
    let node = get_node(&replacer, &frame_id);
    assert_eq!(replacer.node_store.len(), 1);
    assert_eq!(replacer.current_timestamp, current_timestamp + 1);
    assert_eq!(node.history.len(), 1);
    assert_eq!(*node.history.back().unwrap(), current_timestamp);

    current_timestamp += 1;

    // record access on existing frame will push current timestamp to corresponding node's history.
    replacer.record_access(&frame_id, AccessType::Lookup);
    let node = get_node(&replacer, &frame_id);
    assert_eq!(replacer.node_store.len(), 1);
    assert_eq!(replacer.current_timestamp, current_timestamp + 1);
    assert_eq!(node.history.len(), 2);
    assert_eq!(*node.history.back().unwrap(), current_timestamp);
    assert_eq!(*node.history.front().unwrap(), current_timestamp - 1);
}

#[test]
fn test_backwards_k_distance() {
    let mut k = 5_usize;
    let mut replacer = LRUKReplacer::builder().max_size(10).k(k).build();

    let frame_id = get_new_frame_and_record_access(&mut replacer);
    while k > 2 {
        replacer.record_access(&frame_id, AccessType::Lookup);
        assert_eq!(
            get_node(&replacer, &frame_id).get_backwards_k_distance(replacer.current_timestamp),
            INF
        );
        k -= 1;
    }
    for _ in 0..2 {
        replacer.record_access(&frame_id, AccessType::Lookup);
        assert_eq!(
            get_node(&replacer, &frame_id).get_backwards_k_distance(replacer.current_timestamp),
            5
        );
    }
}

pub(crate) fn get_new_frame_and_record_access(replacer: &mut LRUKReplacer) -> FrameId {
    if replacer.is_full_capacity() {
        panic!("Can't get new frame for replacer without evicting an existing frame.");
    }
    // get a frame_id in interval [0, max_size) that is not currently in use.
    let mut new_frame_id = replacer.max_size - 1;
    for frame_id in replacer.node_store.keys() {
        if *frame_id == 0_usize {
            continue;
        }
        if !replacer.node_store.contains_key(&(frame_id - 1)) {
            new_frame_id = frame_id - 1;
            break;
        }
    }
    replacer.record_access(&new_frame_id, DUMMY_ACCESS_TYPE);
    new_frame_id
}

fn get_backwards_k_distance_for_node(replacer: &mut LRUKReplacer, frame_id: &FrameId) -> usize {
    let node = get_node(&replacer, &frame_id);
    node.get_backwards_k_distance(replacer.current_timestamp)
}

fn get_node<'a>(replacer: &'a LRUKReplacer, frame_id: &FrameId) -> &'a LRUKNode {
    replacer
        .node_store
        .get(&frame_id)
        .expect("No node corresponding to {frame_id} exists in replacer node store.")
}

fn random_bool() -> bool {
    let mut rng = rand::thread_rng();
    rng.gen_bool(2.0 / 3.0)
}

/// Record `n` accesses of each `FrameId` in `frame_ids` in order, i.e. the first frame of
/// `frame_ids` has the oldest access and the last frame has the newest access.
fn record_access_frames_n_times(replacer: &mut LRUKReplacer, frame_ids: &Vec<FrameId>, n: usize) {
    frame_ids.iter().for_each(|frame_id| {
        for _ in 0..n {
            replacer.record_access(&frame_id, AccessType::Lookup);
        }
    });
}

fn record_access_frame_n_times(replacer: &mut LRUKReplacer, frame_id: FrameId, n: usize) {
    record_access_frames_n_times(replacer, &vec![frame_id], n);
}

fn set_multiple_frames_evictable(replacer: &mut LRUKReplacer, frame_ids: &Vec<FrameId>) {
    frame_ids
        .iter()
        .for_each(|frame_id| replacer.set_evictable(&frame_id, true));
}
