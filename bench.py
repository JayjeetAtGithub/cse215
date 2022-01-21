import os
import sys
import time

import duckdb
import pyarrow.dataset as ds

if __name__ == "__main__":
    dataset_path = str(sys.argv[1])
    format = str(sys.argv[2])

    lineitem = ds.dataset(os.path.join(dataset_path, "lineitem"), format=format)
    supplier = ds.dataset(os.path.join(dataset_path, "supplier"), format=format)
    customer = ds.dataset(os.path.join(dataset_path, "customer"), format=format)
    region   = ds.dataset(os.path.join(dataset_path, "region"), format=format)
    nation   = ds.dataset(os.path.join(dataset_path, "nation"), format=format)
    orders   = ds.dataset(os.path.join(dataset_path, "orders"), format=format)
    part     = ds.dataset(os.path.join(dataset_path, "part"), format=format)
    partsupp = ds.dataset(os.path.join(dataset_path, "partsupp"), format=format)

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
        print(f"Query {query_no + 1}: ", e - s)
