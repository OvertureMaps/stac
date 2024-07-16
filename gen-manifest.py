import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import os
import json

release_version = "2024-06-13-beta.1"

#The object that we'll eventually serialize into the release-level manifest
json_dict = {}

json_dict['version'] = release_version

# Get the name of a fully-qualified s3 blob storage path assuming our 'thing=stuff' format spec
def parse_name(s3_file_path): 
    return os.path.split(s3_file_path)[1].split('=')[1]

# Generate the theme-specific blocks that go in the top-line manifest
def process_theme(s3fs, theme_info, theme_name):
    theme_dict = {}
    theme_dict['name'] = theme_name;
    print ("Processing " + theme_name + " theme")
    theme_path_selector = fs.FileSelector(theme_info.path)
    theme_dict['path'] = theme_info.path
    theme_info = s3fs.get_file_info(theme_path_selector)
    theme_dict['types'] = []
    
    for type in theme_info: 
        type_name = parse_name(type.path)
        theme_dict['types'].append(type_name)
        print ("\tProcessing Type " + type_name)

    return theme_dict

print ('Generating release manifest for release ' + release_version)
release_path = "overturemaps-us-west-2/release/" + release_version +"/"
json_dict['s3_location'] = release_path

### Look in a specific release to obtain the themes themselves
filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")

release_path_selector = fs.FileSelector(release_path);

themes_info = filesystem.get_file_info(release_path_selector);

theme_info = []

for theme in themes_info:
    theme_name = parse_name(theme.path) 
    theme_info.append(process_theme(filesystem, theme, theme_name))

json_dict['themes'] = theme_info

json_object = json.dumps(json_dict, indent=4)

with open("sample.json", "w") as outfile:
    outfile.write(json_object)

# dataset = ds.dataset(
#     path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
# )


