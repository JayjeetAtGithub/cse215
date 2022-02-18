import os
import sys
import time

import multiprocessing as mp

import duckdb
from pandas.testing import assert_frame_equal

import pyarrow.dataset as ds


def drop_caches():
    os.system("sync")
    os.system("echo 3 > /proc/sys/vm/drop_caches")
    os.system("sync")
    time.sleep(2)


if __name__ == "__main__":
    dataset_path = str(sys.argv[1])
    query_no = int(sys.argv[2])

    format="parquet"
    lineitem = ds.dataset(os.path.join(dataset_path, "lineitem"), format=format)
    supplier = ds.dataset(os.path.join(dataset_path, "supplier"), format=format)
    customer = ds.dataset(os.path.join(dataset_path, "customer"), format=format)
    region   = ds.dataset(os.path.join(dataset_path, "region"), format=format)
    nation   = ds.dataset(os.path.join(dataset_path, "nation"), format=format)
    orders   = ds.dataset(os.path.join(dataset_path, "orders"), format=format)
    part     = ds.dataset(os.path.join(dataset_path, "part"), format=format)
    partsupp = ds.dataset(os.path.join(dataset_path, "partsupp"), format=format)

    with open(f"queries/q{query_no}.sql", "r") as f:
        query = f.read()

    query = f"PRAGMA threads={mp.cpu_count()};\n{query}"
    drop_caches()
    conn = duckdb.connect()
    result_parquet = conn.execute(query).fetchdf()
    print(result_parquet)
    print(result_parquet.info())
    conn.close()

    format="skyhook"
    lineitem = ds.dataset(os.path.join(dataset_path, "lineitem"), format=format)
    supplier = ds.dataset(os.path.join(dataset_path, "supplier"), format=format)
    customer = ds.dataset(os.path.join(dataset_path, "customer"), format=format)
    region   = ds.dataset(os.path.join(dataset_path, "region"), format=format)
    nation   = ds.dataset(os.path.join(dataset_path, "nation"), format=format)
    orders   = ds.dataset(os.path.join(dataset_path, "orders"), format=format)
    part     = ds.dataset(os.path.join(dataset_path, "part"), format=format)
    partsupp = ds.dataset(os.path.join(dataset_path, "partsupp"), format=format)

    with open(f"queries/q{query_no}.sql", "r") as f:
        query = f.read()

    query = f"PRAGMA threads={mp.cpu_count()};\n{query}"
    drop_caches()
    conn = duckdb.connect()
    result_skyhook = conn.execute(query).fetchdf()
    print(result_skyhook)
    print(result_skyhook.info())
    conn.close()

    print(result_parquet.equals(result_skyhook))
    print(assert_frame_equal(result_parquet, result_skyhook))
