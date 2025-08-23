import time
from typing import cast

import numpy as np
import s3fs
import zarr
import zarr.storage

base_url = "http://127.0.0.1:9000"

fs = s3fs.S3FileSystem(
    endpoint_url=base_url,
    key="minioadmin",
    secret="minioadmin",
    use_ssl=True,
    asynchronous=True,
)
store = zarr.storage.FsspecStore(fs, path="test")
top_groups = 100
sub_groups = 10
arrays_per_subgroup = 200
array_shape = (100, 4)
array_dtype = "f4"


def create_large_hierarchy():
    print("Creating large Zarr hierarchy...")
    t0 = time.time()
    root = zarr.group(store)
    for i in range(top_groups):
        g_name = f"group_{i:03d}"
        g = root.require_group(g_name)
        for j in range(sub_groups):
            sg_name = f"subgroup_{j:03d}"
            sg = g.require_group(sg_name)
            for k in range(arrays_per_subgroup):
                arr_name = f"array_{k:03d}"
                arr = sg.create_array(
                    arr_name,
                    shape=array_shape,
                    dtype=array_dtype,
                    overwrite=True,
                )
                arr[:] = np.random.rand(*array_shape).astype(array_dtype)
        if (i + 1) % 10 == 0:
            print(f"Created {i + 1} top-level groups...")
    t1 = time.time()
    print(f"Hierarchy creation (upload) took {t1 - t0:.2f} seconds.")


def list_performance():
    print("Listing all arrays...")
    t0 = time.time()
    root = zarr.open_group(store)
    count = 0
    for g_name in root.group_keys():
        print(g_name)
        g = cast(zarr.Group, root[g_name])
        for sg_name in g.group_keys():
            sg = cast(zarr.Group, g[sg_name])
            for _arr_name in sg.array_keys():
                count += 1
    t1 = time.time()
    print(f"Listed {count} arrays in {t1 - t0:.2f} seconds.")


def download_performance():
    print("Downloading a sample array...")
    root = zarr.open_group(store)
    g = cast(zarr.Group, root["group_000"])
    sg = cast(zarr.Group, g["subgroup_000"])
    arr = cast(zarr.Array, sg["array_000"])
    t0 = time.time()
    _data = arr[:]
    t1 = time.time()
    print(f"Downloaded array shape {arr.shape} in {t1 - t0:.4f} seconds.")


if __name__ == "__main__":
    # create_large_hierarchy()
    list_performance()
    download_performance()
