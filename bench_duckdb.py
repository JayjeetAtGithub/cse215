import os
import pyarrow.dataset as ds
import duckdb
import sys
import time
import json

import multiprocessing as mp


def drop_caches():
    os.system('sync')
    os.system('echo 3 > /proc/sys/vm/drop_caches')
    os.system('sync')


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("usage: ./bench.py <format(pq/sk)> <iterations> <dataset> <file>")
        sys.exit(0)

    fmt = str(sys.argv[1])
    iterations = int(sys.argv[2])
    directory = str(sys.argv[3])
    resultfile = str(sys.argv[4])

    if fmt == "sk":
        format_ = "skyhook"
    elif fmt == "pq":
        format_ = "parquet"

    data = list()
    for i in range(iterations):
        drop_caches()
        dataset_ = ds.dataset(directory, format=format_)
        conn = duckdb.connect()
        query = "SELECT * from dataset_"
        #query = f"PRAGMA threads={mp.cpu_count()};\n{query}"
        start = time.time()
        record_batch_reader = conn.execute(query).fetch_record_batch(1024 * 1024)
        chunk = record_batch_reader.read_next_batch()
        while chunk is not None:
            print(chunk.to_pandas())
            try:
                chunk = record_batch_reader.read_next_batch()
            except:
                break
        end = time.time()
        conn.close()
        data.append(end-start)
        print(end-start)

        with open(resultfile, 'w') as fp:
            json.dump(data, fp)