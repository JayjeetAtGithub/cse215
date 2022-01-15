import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

if __name__ == "__main__":
    table_name = str(sys.argv[1])
    source_dir = str(sys.argv[2])
    destination_dir = str(sys.argv[3])

    # Read the schema file and parse the column names
    with open(f'schemas/{table_name}', 'r') as f:
        lines = f.readlines()
    cols = list()
    for line in lines:
        key, type = line.split(',')
        key = key.strip()
        type = type.strip()
        cols.append(key)
    
    # Read the csv file and convert it to a pyarrow table 
    # applying the provided schema
    parse_options = csv.ParseOptions(delimiter='|')
    read_options = csv.ReadOptions(column_names=cols)
    table = csv.read_csv(os.path.join(source_dir, f'{table_name}.tbl'), read_options=read_options, parse_options=parse_options)
    print(table.schema)

    # Write the table to a parquet file
    pq.write_table(table, os.path.join(destination_dir, f'{table_name}.parquet'), compression='snappy')
    print(f'{table_name}.parquet file created.')