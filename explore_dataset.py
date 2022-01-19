import os
import sys

if __name__ == "__main__":
    dataset_dir = str(sys.argv[1])
    for root, dirs, files in os.walk(dataset_dir):
        for file in files:
            if file.endswith(".parquet"):
                print(f"{os.path.join(root, file)} = {os.stat(os.path.join(root, file)).st_size/(1024*1024)} MB")
