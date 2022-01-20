import os
import sys
import shutil

import pyarrow.parquet as pq


def write_file(filename, table):
    open(filename, 'a').close()
    attribute = "ceph.file.layout.object_size"
    os.system(
        f"setfattr -n {attribute} -v 16777216 {filename}")
    pq.write_table(
        table, filename,
        row_group_size=table.num_rows, compression="snappy"
    )

if __name__ == "__main__":
    dataset_dir = str(sys.argv[1])
    scale = int(sys.argv[2])
    for root, dirs, files in os.walk(dataset_dir):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                print(f"Multiplying {file_path} {scale} times")
                for i in range(scale):
                    print(f'Copying {file_path} to {os.path.join(os.path.dirname(file_path), f"{os.path.basename(file_path)}.{i}.parquet")}')
                    write_file(os.path.join(os.path.dirname(file_path), f"{os.path.basename(file_path)}.{i}.parquet"), pq.read_table(file_path))
