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
    format = str(sys.argv[1])

    data = list()
    dataset_ = ds.dataset("/mnt/cephfs/dataset", format=format)
    selectivity = [1, 10, 25, 50, 75, 90, 99, 100] 

    queries = {
        "1": "SELECT * FROM dataset_ WHERE total_amount > 69",
        "10": "SELECT * FROM dataset_ WHERE total_amount > 27",
        "25": "SELECT * FROM dataset_ WHERE total_amount > 19",
        "50": "SELECT * FROM dataset_ WHERE total_amount > 11",
        "75": "SELECT * FROM dataset_ WHERE total_amount > 9",
        "90": "SELECT * FROM dataset_ WHERE total_amount > 4",
        "99": "SELECT * FROM dataset_ WHERE total_amount > -200",
        "100": "SELECT * FROM dataset_",
    }

    for sel in selectivity:
        drop_caches()
        conn = duckdb.connect()
        s = time.time()
        base_query = f"PRAGMA disable_object_cache;\nPRAGMA threads={mp.cpu_count()};\n{queries[sel]}"
        result = conn.execute(base_query).fetchall()
        e = time.time()
        conn.close()

        log_str = f"{sel}|{format}|{e - s}"
        print(log_str)

        data.append({
            "query": sel,
            "format": format,
            "latency": e - s
        })

    with open(f"nyctaxi_results/bench_result.{sel}.{format}.json", "w") as f:
        f.write(json.dumps(data))
    print("Benchmark finished")
