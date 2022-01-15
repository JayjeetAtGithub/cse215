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
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT
                avg(c_acctbal)
            FROM
                customer
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    o_custkey = c_custkey)) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;""")

    record_batch_reader = query.fetch_record_batch()
    chunk = record_batch_reader.read_next_batch()
    while chunk is not None:
        print(chunk.to_pandas())
        try:
            chunk = record_batch_reader.read_next_batch()
        except:
            break