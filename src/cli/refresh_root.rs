//! `refresh-root` — recompute derived state on the root catalog and write it back.

use overture_stac::stac::refresh_latest;
use overture_stac::storage::{get_json_optional, put_json, Bucket};
use overture_stac::Result;

use super::PROD_CATALOG_URI;

#[derive(clap::Args, Debug)]
pub struct RefreshRootArgs {
    /// Object-store URI to the catalog bucket. Must point at the catalog root.
    #[arg(long = "catalog-uri", default_value = PROD_CATALOG_URI)]
    catalog_uri: String,
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
    put_json(&bucket, root_key, &root).await?;
    println!("Refreshed root `latest` flag.");
    Ok(())
}
