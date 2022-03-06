import os
import sys
import time
import json

import multiprocessing as mp

import duckdb
import pyarrow.dataset as ds


def drop_caches():
    os.system("sync")
    os.system("echo 3 > /proc/sys/vm/drop_caches")
    os.system("sync")
    time.sleep(2)


if __name__ == "__main__":
    dataset_path = str(sys.argv[1])
    query_no = int(sys.argv[2])
    format = str(sys.argv[3])

    data = list()
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

    query = f"PRAGMA disable_object_cache;\nPRAGMA threads={mp.cpu_count()};\n{query}"
    for _ in range(10):
        drop_caches()
        conn = duckdb.connect()
        s = time.time()
        result = conn.execute(query).fetchall()
        e = time.time()
        conn.close()

        log_str = f"{query_no}|{format}|{e - s}"
        print(log_str)

        data.append({
            "query": query_no,
            "format": format,
            "latency": e - s
        })

    with open(f"current_results/bench_result.{query_no}.{format}.json", "w") as f:
        f.write(json.dumps(data))
    print("Benchmark finished")
