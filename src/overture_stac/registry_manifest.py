import logging

import pyarrow.dataset as ds
import pyarrow.fs as fs


class RegistryManifest:
    """
    Class to create a registry manifest by reading all parquet files
    in s3://overturemaps-us-west-2/registry/ and extracting max IDs.
    Returns a compact list of [filename, max_id] tuples sorted by max_id.
    """

    def __init__(
        self,
        registry_path: str = "overturemaps-us-west-2/registry",
        s3_region: str = "us-west-2",
    ):
        self.registry_path = registry_path
        self.filesystem = fs.S3FileSystem(anonymous=True, region=s3_region)

        logging.basicConfig()
        self.logger = logging.getLogger("registry-manifest")
        self.logger.setLevel(logging.INFO)

    def create_manifest(self):
        """
        Read all parquet files in the registry path and create a manifest
        with max IDs and file paths.

        Returns:
            list: Sorted list of [filename, max_id] tuples
        """
        self.logger.info(f"Scanning registry path: {self.registry_path}")

        # Get all files in the registry directory
        registry_selector = fs.FileSelector(self.registry_path, recursive=True)
        all_files = self.filesystem.get_file_info(registry_selector)

        # Filter for parquet files only
        parquet_files = [
            f
            for f in all_files
            if f.path.endswith(".parquet") and f.type == fs.FileType.File
        ]

        self.logger.info(f"Found {len(parquet_files)} parquet files in registry")

        # List to store [filename, min_id] tuples
        manifest_entries = []

        # Process each parquet file
        for file_info in parquet_files:
            try:
                # Create dataset for this single file
                file_dataset = ds.dataset(
                    file_info.path, filesystem=self.filesystem, format="parquet"
                )

                # Get fragments
                fragments = list(file_dataset.get_fragments())

                if not fragments:
                    self.logger.warning(f"No fragments found for {file_info.path}")
                    continue

                # Use first fragment to get schema
                fragment = fragments[0]
                schema = fragment.metadata.schema.to_arrow_schema()

                # Check if 'id' column exists
                has_id_column = "id" in [field.name for field in schema]

                if has_id_column:
                    # Find the ID column index
                    id_column_index = next(
                        i for i, field in enumerate(schema) if field.name == "id"
                    )

                    # Get max from last row group of first fragment
                    first_fragment = fragments[0]
                    first_metadata = first_fragment.metadata

                    if first_metadata.num_row_groups > 0:
                        # Access the LAST row group to get max_id
                        last_row_group_index = first_metadata.num_row_groups - 1
                        last_row_group = first_metadata.row_group(last_row_group_index)
                        last_id_column = last_row_group.column(id_column_index)

                        if (
                            last_id_column.statistics
                            and last_id_column.statistics.has_min_max
                        ):
                            max_id = last_id_column.statistics.max
                            if isinstance(max_id, bytes):
                                max_id = max_id.decode("utf-8")

                            filename = file_info.path.replace(
                                "overturemaps-us-west-2/registry/", ""
                            )
                            manifest_entries.append([filename, max_id])

                            self.logger.info(
                                f"{file_info.path.split('/')[-1]}: Max ID: {max_id}"
                            )
                else:
                    self.logger.warning(f"No 'id' column found in {file_info.path}")

            except Exception as e:
                self.logger.error(f"Error processing {file_info.path}: {e}")

        # Sort by max_id for efficient lookup
        manifest_entries.sort(key=lambda x: x[1])

        self.logger.info(f"Total files processed: {len(manifest_entries)}")

        return manifest_entries
