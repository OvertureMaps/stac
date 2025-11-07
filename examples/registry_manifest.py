"""
Look up which registry file contains a given ID.
"""

import bisect

# ID you want to find
search_id = "335f69e3-699d-44ac-98c4-f0e81bc66680"


def get_registry_file(registry: dict, search_id: str):
    manifest = registry.get("manifest")

    max_ids = [entry[1] for entry in manifest]

    # Use bisect_left to find insertion point
    index = bisect.bisect_left(max_ids, search_id)

    # Find the file that might contain this ID
    if index < len(max_ids):
        if max_ids[index] == search_id:
            # Exact match
            filename = manifest[index][0]
        elif max_ids[index] > search_id:
            # ID is less than this max_id, so check this file
            filename = manifest[index][0]
        else:
            # ID is beyond this file, check next
            if index + 1 < len(max_ids):
                filename = manifest[index + 1][0]
            else:
                filename = None
    else:
        # Beyond all files
        filename = None

    return f"{registry.get('path')}/{filename}"


# In practice, fetch catalog from https://stac.overturemaps.org/catalog.json
# registry = catalog.get("registry")

registry = {
    "path": "s3://overturemaps-us-west-2/registry",
    "manifest": [
        [
            "part-00000-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "0607e6db-24ae-49b8-a35c-5543b3ad7490",
        ],
        [
            "part-00001-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "0b4f5cef-8b36-4ede-9a6a-fbfb6ae855f1",
        ],
        [
            "part-00002-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "0fc36099-b424-4eaf-8df2-ef4c3811f92e",
        ],
        [
            "part-00003-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "158c2fe6-6121-4f88-815e-32ec2d9ec059",
        ],
        [
            "part-00004-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "19df406e-a5eb-4abc-9f38-9926055d056d",
        ],
        [
            "part-00005-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "1ef898c0-1fb7-4cca-ba54-78b0c29e4103",
        ],
        [
            "part-00006-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "22f352db-8bd9-4f75-8681-432ca1b35a00",
        ],
        [
            "part-00007-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "28d5deb5-2e00-4be8-ab80-46ac5b661966",
        ],
        [
            "part-00008-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "2e8677b9-b45d-4006-92a4-8c3c32a9c7a8",
        ],
        [
            "part-00009-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "33053bfe-d4c1-49e7-914b-85bdf55616fc",
        ],
        [
            "part-00010-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "384020a4-990c-408f-9055-8e1104855845",
        ],
        [
            "part-00011-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "3d62e6c1-99a7-4a7c-823b-2111bb6f6f82",
        ],
        [
            "part-00012-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "4304b1fe-088b-4119-b76b-b9232195febb",
        ],
        [
            "part-00013-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "4897b99d-9f26-40ad-9a94-34617a19fe73",
        ],
        [
            "part-00014-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "4d244a4b-9d6b-48c8-a2db-d6d69c0db7a9",
        ],
        [
            "part-00015-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "5286c73b-7dc9-4209-8b65-172f55fd205c",
        ],
        [
            "part-00016-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "5811924d-4066-4abd-8487-9980e819989a",
        ],
        [
            "part-00017-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "5e09a629-0054-4050-a120-7181c5132cb8",
        ],
        [
            "part-00018-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "629f4df0-25cc-4610-a844-b5aa380c4ea6",
        ],
        [
            "part-00019-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "670e0361-3946-45e2-929f-9aa395bd07f7",
        ],
        [
            "part-00020-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "6dc9eb77-9bc4-4b98-b967-f3efe095072d",
        ],
        [
            "part-00021-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "7383886d-87b7-421f-a940-4f80dd855d2b",
        ],
        [
            "part-00022-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "787bffc1-9b3d-4bf0-9da6-28c105d7ecdb",
        ],
        [
            "part-00023-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "7dfc8b2d-b516-45f6-bd95-3c5e6b7ef61b",
        ],
        [
            "part-00024-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "833313a5-ada9-4543-97c5-cc685c3c7374",
        ],
        [
            "part-00025-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "87ed9e66-97ed-4486-a45c-349a6bff2411",
        ],
        [
            "part-00026-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "8d1a1734-8400-4f99-a251-4da056928349",
        ],
        [
            "part-00027-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "91c8bc04-1696-416b-b633-dbf5f71fd459",
        ],
        [
            "part-00028-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "96acedd3-cbd2-43f2-a411-1eec5a44769b",
        ],
        [
            "part-00029-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "9c3179da-4452-4938-bb6d-5b457379deaa",
        ],
        [
            "part-00030-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "a0dcaa3e-a922-42b9-9b06-b6f616115c78",
        ],
        [
            "part-00031-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "a57f8da9-4fee-4f0a-91d8-9c9ca7c415ec",
        ],
        [
            "part-00032-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "ab1022f7-6cb2-44b5-a02b-7ccc21295d1f",
        ],
        [
            "part-00033-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "b015f99a-deae-47c5-821c-6e26c957785b",
        ],
        [
            "part-00034-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "b5014811-09f0-4652-ba41-f8db9b54aada",
        ],
        [
            "part-00035-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "ba773c11-cb07-4f80-b070-bb8641a37f07",
        ],
        [
            "part-00036-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "bf8559cf-d36c-4f4a-9672-4b0ad8373c2c",
        ],
        [
            "part-00037-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "c4fcadd5-cf65-4f35-8a7a-9b7c5ef6f806",
        ],
        [
            "part-00038-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "c9e8c59d-8e1f-42dd-8c91-4f9f70037f05",
        ],
        [
            "part-00039-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "ce7a8262-53a2-4227-837c-582cbec72f9b",
        ],
        [
            "part-00040-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "d384536a-d670-4111-99d3-8d2058cf7966",
        ],
        [
            "part-00041-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "d87a6fec-7075-4a0a-937a-670372c9887e",
        ],
        [
            "part-00042-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "de58e683-09df-4a04-8a83-d1bb8793c8ca",
        ],
        [
            "part-00043-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "e3361080-4d93-43bc-8578-4fbe7e210747",
        ],
        [
            "part-00044-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "e839d706-a1a7-43fd-8cb3-471bc63eb797",
        ],
        [
            "part-00045-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "ed1a7de2-f535-4b45-9def-3b07a9562b3e",
        ],
        [
            "part-00046-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "f1840537-49ad-4fea-91a4-9fff030f89ec",
        ],
        [
            "part-00047-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "f5f7435a-d9ef-4a49-9e2b-5159dd9d74aa",
        ],
        [
            "part-00048-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "fb32d7cd-18b8-428d-aaa4-bb51601c20a8",
        ],
        [
            "part-00049-0c77d616-e46a-448d-8d19-21c71084c570-c000.zstd.parquet",
            "fffffffe-25d0-47c2-a17d-f6e006457174",
        ],
    ],
}

print(get_registry_file(registry, search_id))
