# Overture STAC

This repo serves two functions:

1) to generate STAC Catalogs for a single overture release.
2) To generate download manifests for the explore site.

## To generate a STAC Catalog:

The required arguments are `--release=<RELEASE>` and `--schema=<VERSION>` (without the leading v), for example:

The script will trawl through the public release folder and emit the STAC catalog contents in the `stac` directory.

Optionally, specify `--output=<PATH>` to write somewhere other than `stac`, and `--parquet=<PATH>` will write a per-type StacGeoParquet file at that path.

Example:
```bash
python3 gen-data-release-stac.py --release=2025-05-21.0 --schema=1.9.0 --output=stac --parquet=stac-parquet
```
Writes a complete STAC catalog to the `stac` directory, and a per-type StacGeoParquet file to the the `stac-parquet` directory.


## To generate the explore site manifest:
Same as above, but instead you will modify / run the `gen-explore-site-manifest.py` script instead.

The manifest is stored in a file called `{release_version}.json` when done.



## Virtual Environment
To enter the venv:

cd to the base dir.
> virtualenv -p python3 .
> source   bin/activate
> python3 ./gen-manifest.py

Then pip install any modules you're missing.

To exit the venv:

> deactivate
