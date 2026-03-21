pub mod error;
pub mod cluster;

use ofi_libfabric_sys::bindgen as ffi;

// --- Chitchat key constants ---

pub const CLUSTER_ID: &str = "blitzdb-demo";
pub const KEY_EP_ADDR: &str = "ep_addr";
pub const KEY_INDEX_MR_KEY: &str = "index_mr_key";
pub const KEY_HEAP_MR_KEY: &str = "heap_mr_key";
pub const KEY_NUM_KEYS: &str = "num_keys";

pub use error::FabricError;
pub type Result<T> = std::result::Result<T, FabricError>;
