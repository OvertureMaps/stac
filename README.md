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

The top-level `catalog.json` intends to be a catalog of all publicly available Overture releases. Briefly, it looks like this: 
```json
{
  "type": "Catalog",
  "id": "Overture Releases",
  "stac_version": "1.1.0",
  "description": "All Overture Releases",
  "links": [
    {
      "rel": "child",
      "href": "./2025-07-23.0/catalog.json",
      "type": "application/json",
      "title": "Latest Overture Release",
      "latest": true
    },
    {
      "rel": "child",
      "href": "./2025-06-25.0/catalog.json",
      "type": "application/json",
      "title": "2025-06-25.0 Overture Release"
    },
  ],
  "latest": "2025-07-23.0"
}
```
The top level catalog points to the `latest` Overture release, and this release also has the tag `latest:true`. 

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
