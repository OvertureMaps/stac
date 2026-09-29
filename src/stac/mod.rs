//! STAC document construction.
//!
//! - [`catalog`]: release + top-level catalog assembly, absolute-published save.
//! - [`theme`]: per-theme fan-out — reads parquet fragments, builds Collection/Item values.
//! - [`registry`]: registry manifest (max-ID per parquet).
//! - [`pmtiles`]: discovery of per-release PMTiles files.
//! - [`root`]: umbrella `catalog.json` parsing/mutation (child links, VCS stamp).
//!
//! Everything callers commonly need is re-exported at this level.

pub mod catalog;
pub mod pmtiles;
pub mod registry;
pub mod root;
pub mod theme;
pub mod validate;

pub use catalog::{
    build_single_release, build_top_catalog, compute_neighbor_links, link_neighbor_releases,
    list_release_ids, save_root_catalog, save_sub_catalog, validate_release_id, ReleaseCatalog,
};
pub use root::{
    add_child_link, build_empty_root, children_from_root, read_catalog_children, refresh_latest,
    release_id_from_href, remove_child_link, stamp_vcs, ROOT_CATALOG_TITLE,
};
pub use validate::{
    validate_catalog, validate_catalog_uri, validate_url, Failure, FailureKind, ValidateOptions,
    ValidationReport,
};
