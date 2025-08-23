import os
import time

import numpy as np
from netCDF4 import Dataset

# Configuration
file_path = "benchmark_netcdf4.nc"
object_size = 128 * 1024  # bytes per "object"
object_count = 1000


def random_bytes(size):
    return np.frombuffer(os.urandom(size), dtype=np.uint8)


def benchmark_write(file_path, size, count):
    print(
        f"Starting NetCDF4 write benchmark: {count} objects of {size // (1024 * 1024)}MB each"
    )
    start = time.time()
    with Dataset(file_path, "w", format="NETCDF4") as ds:
        ds.createDimension("obj", count)
        ds.createDimension("byte", size)
        var = ds.createVariable("data", "u1", ("obj", "byte"))
        for i in range(count):
            data = random_bytes(size)
            var[i, :] = data
            if (i + 1) % 100 == 0:
                print(f"  Wrote {i + 1}/{count} objects")
    elapsed = time.time() - start
    mb_total = (size * count) / (1024 * 1024)
    print(f"Write benchmark finished in {elapsed:.2f} seconds")
    print(
        f"Write throughput: {mb_total / elapsed:.2f} MB/s, {count / elapsed:.2f} objects/s"
    )
    return elapsed


def benchmark_read(file_path, size, count):
    print(
        f"Starting NetCDF4 read benchmark: {count} objects of {size // (1024 * 1024)}MB each"
    )
    start = time.time()
    with Dataset(file_path, "r") as ds:
        var = ds.variables["data"]
        for i in range(count):
            data = var[i, :]
            assert data.shape[0] == size, f"Read size mismatch for object {i}"
            if (i + 1) % 100 == 0:
                print(f"  Read {i + 1}/{count} objects")
    elapsed = time.time() - start
    mb_total = (size * count) / (1024 * 1024)
    print(f"Read benchmark finished in {elapsed:.2f} seconds")
    print(
        f"Read throughput: {mb_total / elapsed:.2f} MB/s, {count / elapsed:.2f} objects/s"
    )
    return elapsed


def main():
    write_time = benchmark_write(file_path, object_size, object_count)
    read_time = benchmark_read(file_path, object_size, object_count)
    print("\nBenchmark summary:")
    print(
        f"  Write: {object_count} objects x {object_size // (1024 * 1024)}MB in {write_time:.2f}s"
    )
    print(
        f"  Read:  {object_count} objects x {object_size // (1024 * 1024)}MB in {read_time:.2f}s"
    )


if __name__ == "__main__":
    main()
