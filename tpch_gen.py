import os
import sys

import multiprocessing as mp

from concurrent.futures import ThreadPoolExecutor


def generate(index, num_procs):
    os.system(f"dbgen -vf -s 100 -C {num_procs} -S {index} -T {table} -f")


if __name__ == "__main__":
    table = str(sys.argv[1])
    os.environ["DSS_PATH"] = "/mnt/cephfs/tpch_sf100"
    num_procs = mp.cpu_count()
    with ThreadPoolExecutor(max_workers=num_procs) as executor:
        for index in range(num_procs):
            executor.map(generate, index, num_procs, table)
