//! `list-releases` — print the release IDs the data bucket currently exposes.

use overture_stac::stac::list_release_ids;
use overture_stac::storage::Bucket;
use overture_stac::Result;

use super::PROD_DATA_URI;

#[derive(clap::Args, Debug)]
pub struct ListReleasesArgs {
    /// Object-store URI to the data bucket (s3://, gs://, az://, file:// ...).
    #[arg(long = "data-uri", default_value = PROD_DATA_URI)]
    data_uri: String,
}

pub async fn run(args: ListReleasesArgs) -> Result<()> {
    let bucket = Bucket::from_url(&args.data_uri)?;
    let ids = list_release_ids(&bucket, "release").await?;
    for id in ids {
        println!("{id}");
    }
    Ok(())
}
