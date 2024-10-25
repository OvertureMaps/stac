import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import os
import json
import yaml
import requests
from datetime import datetime

#release_version = "2024-06-13-beta.1"
pmtiles_release_root = "overturemaps-tiles-us-west-2-beta"

def get_release_date_time(release_date):
    fmt_str = '%Y-%m-%d'
    return datetime.strptime(release_date, fmt_str)

def process_pmtiles_folder(pmtiles_catalog, s3fs, folder): 
    print ( "Processing" + folder.base_name)
    release_folder_selector = fs.FileSelector(folder.path)
    release_id = os.path.split(folder.path)[1]
    
    release_info = {
        'release_id' : release_id,
        'files' : []
    }

    release_files = s3fs.get_file_info(release_folder_selector) 
    pmtiles_catalog
    for pmtiles_file in release_files:
        if (pmtiles_file.is_file):           
            theme_name = pmtiles_file.base_name.split('.')[0]

            release_info['files'].append({
                'theme' : theme_name,
                'href' : "./" + folder.base_name + "/" + pmtiles_file.base_name,
            })
            
    pmtiles_catalog['releases'].append(release_info)


print ('Generating complete catalog for overture PMTiles:')


### Look in a specific release to obtain the themes themselves
filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")

release_path_selector = fs.FileSelector(pmtiles_release_root + "/");

pmtiles_filelist = filesystem.get_file_info(release_path_selector);
pmtiles_catalog = {
   'id':'pmtiles', 
   "url": "https://" + pmtiles_release_root + "-us-west-2.s3.amazonaws.com/",
   "releases" : []
}


for item in pmtiles_filelist:
    if (not item.is_file and 'beta' not in item.base_name and 'alpha' not in item.base_name and 'stac' not in item.base_name):
        process_pmtiles_folder(pmtiles_catalog, filesystem, item)


json_object = json.dumps(pmtiles_catalog, indent=4)

with open("sample.json", "w") as outfile:
    outfile.write(json_object)

