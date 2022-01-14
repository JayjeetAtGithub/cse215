import os

import duckdb
import pyarrow.dataset as ds

if __name__ == "__main__":
    lineitem = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "lineitem.parquet"), format="parquet")
    supplier = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "supplier.parquet"), format="parquet")
    customer = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "customer.parquet"), format="parquet")
    region   = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "region.parquet"), format="parquet")
    nation   = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "nation.parquet"), format="parquet")
    orders   = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "orders.parquet"), format="parquet")
    part     = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "part.parquet"), format="parquet")

    conn = duckdb.connect()

    query = conn.execute("SELECT * FROM lineitem")

    record_batch_reader = query.fetch_record_batch()
    chunk = record_batch_reader.read_next_batch()
    while chunk is not None:
        print(chunk.to_pandas())
        chunk = record_batch_reader.read_next_batch()