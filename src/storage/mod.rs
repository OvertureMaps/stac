//! Object-store and HTTP I/O layer.
//!
//! - [`bucket`]: cloud-agnostic bucket handle over `object_store` (S3, GCS, Azure, file, HTTP).
//! - [`parquet`]: fast fragment-metadata reader over `object_store`.
//! - [`http`]: HTTP fetches against a published STAC catalog.
//!
//! Everything callers commonly need is re-exported at this level.

pub mod bucket;
pub mod http;
pub mod parquet;

pub use bucket::{
    delete_prefix, get_json, list_all, list_top_level, put_bytes, put_json, upload_directory,
    Bucket,
};
pub use http::fetch_schema_version;
pub use parquet::{read_fragment, FragmentInfo};
