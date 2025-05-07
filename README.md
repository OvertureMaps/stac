# stac
This repo serves two functions: 
1) to generate STAC Catalogs for a single overture release.
2) To generate download manifests for the explore site.

## To generate a STAC Catalog: 

Modify the gen-data-release-stac.py so that the release_version is correct: 

`release_version = "2025-04-23.0"`

Then execute the script: 
`python3 ./gen-data-release-stac.py`

The script will trawl through the public release folder and emit the STAC catalog contents in the `build` directory. 

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
