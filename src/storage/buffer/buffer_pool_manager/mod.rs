mod buffer_pool_manager;
#[cfg(test)]
mod tests;

pub use buffer_pool_manager::{BufferPoolManager, BufferPoolManagerBuilder, FrameId};
