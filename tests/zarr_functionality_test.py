import numpy as np
import s3fs
import zarr
import zarr.storage

# Replace with your Rust object storage base URL
base_url = "http://127.0.0.1:9000"

fs = s3fs.S3FileSystem(
    endpoint_url=base_url,
    key="minioadmin",
    secret="minioadmin",
    use_ssl=True,
    asynchronous=True,
)
store = zarr.storage.FsspecStore(fs, path="test")
array_path = "myarray"


def write_read_test():
    data1 = np.arange(100).reshape(10, 10)
    z = zarr.create_array(
        store,
        name=array_path,
        shape=(10, 10),
        dtype="i4",
        overwrite=True,
    )
    print("Writing data to Zarr array...")
    z[:] = data1
    z_read = zarr.open_array(store, path=array_path, mode="r")
    print(z_read[:])
    for k in zarr.open_group(store).array_keys():
        print(f"Array key: {k}")

    assert np.array_equal(z_read[:], data1), "Write-Read test failed"


def write_overwrite_read_test():
    root = zarr.open_group(store)
    print(list(root.array_keys()))
    del root[array_path]
    print(list(root.array_keys()))

    data2 = np.arange(100, 200).reshape(50, 2)
    z = zarr.create_array(
        store, name=array_path, shape=(50, 2), dtype="i4", overwrite=True
    )
    z[:] = data2
    z_read = zarr.open_array(store, mode="r", path=array_path)
    print(z_read[0, 0])
    assert np.array_equal(z_read[:], data2), "Write-Overwrite-Read test failed"


def list_test():
    root = zarr.open_group(store)
    print(list(root.group_keys()))
    for k in root.group_keys():
        print(f"Group key: {k}")

    for k in root.array_keys():
        print(f"Array key: {k}")

    # Check if the array exists
    if array_path in root:
        print(f"Array '{array_path}' exists.")
    else:
        print(f"Array '{array_path}' does not exist.")


if __name__ == "__main__":
    # write_read_test()
    write_overwrite_read_test()
    list_test()
