# Overture STAC

This repo serves two functions:

1) Generate STACs for all public Overture releases
2) To generate download manifests for the explore site.

## To generate the STAC:

```bash
python3 gen-all-release-stac.py`
```

This will generate a complete catalog for the last 5 public releases (trial) at the `public_releases` directory.


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
