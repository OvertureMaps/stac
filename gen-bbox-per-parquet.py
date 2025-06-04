import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs

release_version = "2024-06-13-beta.1"
theme_path = "/theme=buildings"
type_path = "/type=building"

file_loc = "overturemaps-us-west-2/release/" + release_version + theme_path + type_path
print("examining " + file_loc + " metadata.")

### Look in a specific release to obtain the themes themselves
filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")

file_selector = fs.FileSelector(file_loc)

file_info = filesystem.get_file_info(file_selector)

# dataset = ds.dataset(
#     path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
# )

dataset = ds.dataset(
    file_loc, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
)

metadata = dataset.schema.metadata[b"geo"]

meta_str = metadata.decode("utf-8")

metadata_obj = json.loads(meta_str)

bbox_string = json.dumps(metadata_obj["columns"]["geometry"]["bbox"])
version = metadata_obj["version"]
col_names = dataset.schema.names
# Do we need to include/serialize the column formats?
# col_formats = dataset.schema.types
print("bbox: " + bbox_string)


# example value: b'{"version":"1.0.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Polygon","MultiPolygon"],"bbox":[-179.9999966,-83.6500139,-2.8229824,-0.00313771110055319]}}}'
