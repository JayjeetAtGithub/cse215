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

    query = conn.execute("""
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;""")

    record_batch_reader = query.fetch_record_batch()
    chunk = record_batch_reader.read_next_batch()
    while chunk is not None:
        print(chunk.to_pandas())
        try:
            chunk = record_batch_reader.read_next_batch()
        except:
            break