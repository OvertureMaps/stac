//! `refresh-root` — recompute derived state on the root catalog and write it back.

use overture_stac::stac::{check_duckdb_cli, refresh_latest, sync_latest_duckdb};
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

    /// Build and publish `latest.ddb` (needs the DuckDB CLI). Without it, the `latest.ddb`
    /// root link is removed when `latest` changes, so it never points at an older release.
    #[arg(long = "generate-latest-ddb", default_value_t = false)]
    generate_latest_ddb: bool,
}

pub async fn run(args: RefreshRootArgs) -> Result<()> {
    if args.generate_latest_ddb {
        check_duckdb_cli()?;
    }
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
    // Upload the database first so the root never links to a missing file.
    let data_bucket = if args.generate_latest_ddb {
        Some(Bucket::from_url(&args.data_uri)?)
    } else {
        None
    };
    let data = data_bucket.as_ref().map(|b| (b, args.data_uri.as_str()));
    sync_latest_duckdb(&bucket, data, &before, &mut root).await?;
    if root == before {
        println!("Root catalog already consistent.");
        return Ok(());
    }
    put_json(&bucket, root_key, &root).await?;
    println!("Refreshed root `latest` flag.");
    Ok(())
}
