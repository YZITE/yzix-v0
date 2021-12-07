#![forbid(
    unsafe_code,
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion
)]

pub mod build_graph;
pub mod proto;
pub mod store;

pub use camino::{Utf8Path, Utf8PathBuf};
pub use ciborium;
