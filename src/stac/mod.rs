//! STAC document construction.
//!
//! - [`catalog`]: release + top-level catalog assembly, absolute-published save.
//! - [`theme`]: per-theme fan-out — reads parquet fragments, builds Collection/Item values.
//! - [`registry`]: registry manifest (max-ID per parquet).
//! - [`pmtiles`]: discovery of per-release PMTiles files.
//!
//! Everything callers commonly need is re-exported at this level.

pub mod catalog;
pub mod pmtiles;
pub mod registry;
pub mod theme;

pub use catalog::{
    build_single_release, build_top_catalog, link_neighbor_releases, list_release_ids,
    save_absolute_published, ReleaseCatalog,
};
