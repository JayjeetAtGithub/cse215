# Benchmark Instructions

1. Clone the TPCH kit github repository.
```
git clone https://github.com/uccross/tpch-kit
cd tpch-kit
./run.sh
```

2. Convert the CSV files to Parquet format.
```
python3 csv_to_parquet.py /mnt/cephfs/tpch_sf100 /mnt/cephfs/tpch_sf100_parquet
```

3. Split the large parquet files (> 16MB) into smaller chunks.
```
python3 split_dataset.py /mnt/cephfs/tpch_sf100_parquet
```
