import os
import pyarrow.dataset as ds
import sys
import time
import json


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

    selectivity = ["100", "10", "1"]
    data = dict()
    for per in selectivity:
        data[per] = list()
        for i in range(iterations):
            drop_caches()
            dataset_ = ds.dataset(directory, format=format_)
            if per == "100":
                filter_ = None
            if per == "10":
                filter_ = (ds.field("total_amount") > 27)
            if per == "1":
                filter_ = (ds.field("total_amount") > 69)
            start = time.time()
            print(ds.Scanner.from_dataset(dataset_, filter=filter_).to_reader().read_all().num_rows)
            end = time.time()
            data[per].append(end-start)
            print(end-start)

            with open(resultfile, 'w') as fp:
                json.dump(data, fp)