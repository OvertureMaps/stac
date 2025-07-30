# Overture STAC
Generate STACs for all public Overture releases

See it in action here: 
https://radiantearth.github.io/stac-browser/#/external/labs.overturemaps.org/stac/catalog.json?.language=en

The structure looks like this: 

```
- catalog.json
- <RELEASE>/
  | - catalog.json
  | - <THEME>/
      | - catalog.json
      | - <TYPE>/
          | - collection.json
          | - 00001/
              | - 00001.json
          | - 00002/
              | - 00002.json
```

The top-level `catalog.json` intends to be a catalog of all publicly available Overture release.  a catalog of all publicly available 

## Additional Files
At the root of each release, there are two additional files: `manifest.geojson` and `collections.parquet`
```
- <RELEASE>/
  | - manifest.geojson
  | - collections.parquet
```

#### `manifest.geojson`:  Basic GeoJSON Manifest 
To support the download functionality of `explore.overturemaps.org`, a basic `manifest.geojson` summary of the distribution is available at the root of a release.

#### `collections.parquet`: STAC GeoParquet 
An Overture release is composed of nearly 500 individual parquet files, and therefore the STAC index is composed of nearly 500 individual `json` files. This single `collections.parquet` is created by the [`stac-geoparquet`](https://github.com/stac-utils/stac-geoparquet) utility.


## Running

```bash
pip install -r requirements
python3 gen-all-release-stac.py`
```
