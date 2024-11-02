mod lru_k_replacer;
#[cfg(test)]
mod tests;

pub use lru_k_replacer::{AccessType, LRUKReplacer, LRUKReplacerBuilder};
