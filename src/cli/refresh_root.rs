//! `refresh-root` — recompute derived state on the root catalog and write it back.

use overture_stac::stac::{latest_duckdb_stale, publish_latest_duckdb, refresh_latest};
use overture_stac::storage::{get_json_optional, put_json, Bucket};
use overture_stac::Result;

use super::{PROD_CATALOG_URI, PROD_DATA_URI};

#[derive(clap::Args, Debug)]
pub struct RefreshRootArgs {
    /// Object-store URI to the catalog bucket. Must point at the catalog root.
    #[arg(long = "catalog-uri", default_value = PROD_CATALOG_URI)]
    catalog_uri: String,

    /// Object-store URI to the data bucket that `latest.ddb` views read from.
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,
}

pub async fn run(args: RefreshRootArgs) -> Result<()> {
    let bucket = Bucket::from_url(&args.catalog_uri)?;
    let root_key = "catalog.json";
    let Some(mut root) = get_json_optional(&bucket, root_key).await? else {
        println!(
            "No catalog.json at {}. Nothing to refresh.",
            args.catalog_uri
        );
        return Ok(());
    };
    let before = root.clone();
    refresh_latest(&mut root);
    if root == before {
        println!("Root catalog already consistent.");
        return Ok(());
    }
    // Upload the database first so the root never links to a missing file.
    if latest_duckdb_stale(&before, &root) {
        if let Some(latest) = root.get("latest").and_then(|v| v.as_str()) {
            let data_bucket = Bucket::from_url(&args.data_uri)?;
            publish_latest_duckdb(&bucket, &data_bucket, &args.data_uri, latest).await?;
            println!("Published latest.ddb for {latest}.");
        }
    }
    put_json(&bucket, root_key, &root).await?;
    println!("Refreshed root `latest` flag.");
    Ok(())
}
