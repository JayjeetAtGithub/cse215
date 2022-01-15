import os
import time

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
    partsupp = ds.dataset(os.path.join("/mnt/cephfs/tpch_parquet", "partsupp.parquet"), format="parquet")

    conn = duckdb.connect()

    query_files = os.listdir('queries')
    for query_no, query_file in enumerate(query_files):
        with open('queries/' + query_file, 'r') as f:
            query = f.read()

        s = time.time()
        query = conn.execute(query)
        
        record_batch_reader = query.fetch_record_batch()

        try:
            chunk = record_batch_reader.read_next_batch()
        except:
            pass
        while chunk is not None:
            print(chunk.to_pandas())
            try:
                chunk = record_batch_reader.read_next_batch()
            except:
                break
        e = time.time()
        print(f"query {query_no + 1}: ", e - s)
