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


if __name__ == "__main__":
    os.remove("bench.log")
    dataset_path = str(sys.argv[1])
    data = list()
    formats = ["parquet", "skyhook"]
    for format in formats:
        lineitem = ds.dataset(os.path.join(dataset_path, "lineitem"), format=format)
        supplier = ds.dataset(os.path.join(dataset_path, "supplier"), format=format)
        customer = ds.dataset(os.path.join(dataset_path, "customer"), format=format)
        region   = ds.dataset(os.path.join(dataset_path, "region"), format=format)
        nation   = ds.dataset(os.path.join(dataset_path, "nation"), format=format)
        orders   = ds.dataset(os.path.join(dataset_path, "orders"), format=format)
        part     = ds.dataset(os.path.join(dataset_path, "part"), format=format)
        partsupp = ds.dataset(os.path.join(dataset_path, "partsupp"), format=format)

        for i in range(22):
            query_no = i + 1
            with open(f"queries/q{query_no}.sql", "r") as f:
                query = f.read()

            conn = duckdb.connect()
            query = f"PRAGMA threads={mp.cpu_count()};\n{query}"
            for _ in range(5):
                drop_caches()
                s = time.time()
                query_cursor = conn.execute(query)
                record_batch_reader = query_cursor.fetch_record_batch()
                try:
                    chunk = record_batch_reader.read_next_batch()
                except Exception as e:
                    pass
                while chunk is not None:
                    print(chunk.to_pandas())
                    try:
                        chunk = record_batch_reader.read_next_batch()
                    except:
                        break
                e = time.time()
                
                log_str = f"{query_no}|{format}|{e - s}"
                print(log_str)
                with open("bench.log", "a") as f:
                    f.write(log_str + "\n")
                
                data.append({
                    "query": query_no,
                    "format": format,
                    "latency": e - s
                })
            
    with open(f"result.json", "w") as f:
        f.write(json.dumps(data))
    print("Benchmark finished")
