import os
from pprint import pprint

import pyarrow
import pyarrow.parquet as pq


CURR_DIR = os.path.dirname(
    os.path.realpath(__file__)
)

DATA_DIR = os.path.join(
    CURR_DIR, 'data'
)

parquet_file = os.path.join(DATA_DIR, '5_sales.parquet')

pf_obj = pq.ParquetFile(parquet_file)


def print_metadata():
    print("Parquet metadata:")
    print(pf_obj.metadata)


def print_schema():
    print("Parquet schema:")
    print(pf_obj.schema)


def read_row_group():
    pa_table = pf_obj.read_row_group(0)
    pa_table: pyarrow.table.Table
    print(pa_table)
    print(pa_table.columns)


def read_parquet():
    for batch in pf_obj.iter_batches():
        batch: pyarrow.RecordBatch
        pprint(batch.to_pydict())


if __name__ == '__main__':
    # print_metadata()
    # print_schema()
    read_row_group()
    # read_parquet()
