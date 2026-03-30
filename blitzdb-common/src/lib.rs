pub mod error;
pub mod cluster;
pub mod logging;
pub mod endpoint;
mod driver;
mod op;

pub use endpoint::{FabricEndpoint, FabricMrGuard};
pub use op::ReadFuture;

use std::hash::Hash;
use std::io::Write;
use ofi_libfabric_sys::bindgen as ffi;

// --- Chitchat key constants ---

pub const CLUSTER_ID: &str = "blitzdb-demo";
pub const KEY_EP_ADDR: &str = "ep_addr";
pub const KEY_INDEX_MR_KEY: &str = "index_mr_key";
pub const KEY_HEAP_MR_KEY: &str = "heap_mr_key";
pub const KEY_NUM_KEYS: &str = "num_keys";

pub use error::FabricError;
pub type Result<T> = std::result::Result<T, FabricError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct IndexEntry {
    pub offset: u64,
    pub len: u32,
    pub hash: u32,
}

impl IndexEntry {
    pub fn new(item: &[u8], offset: u64, len: u32) -> Self {
        let hash = twox_hash::xxhash3_64::Hasher::oneshot(item) as u32;
        Self { hash, offset, len }
    }

    pub fn matches(&self, item: &[u8]) -> bool {
        self.hash == twox_hash::xxhash3_64::Hasher::oneshot(item) as u32
    }

    pub fn write_to<T: Write>(&self, out: &mut T) -> std::io::Result<()> {
        out.write_all(&self.offset.to_le_bytes())?;
        out.write_all(&self.len.to_le_bytes())?;
        out.write_all(&self.hash.to_le_bytes())?;
        Ok(())
    }
}

impl std::fmt::Display for IndexEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexEntry {{ hash: {:x}, offset: {}, len: {} }}", self.hash, self.offset, self.len)
    }
}