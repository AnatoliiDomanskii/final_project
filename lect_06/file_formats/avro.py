import os
from pprint import pprint

import fastavro

CURR_DIR = os.path.dirname(
    os.path.realpath(__file__)
)

DATA_DIR = os.path.join(
    CURR_DIR, 'data'
)

avro_file = os.path.join(DATA_DIR, '4_sales.avro')


def print_schema():
    with open(avro_file, 'rb') as file_obj:
        avro_reader = fastavro.reader(file_obj)

        print('Avro schema:')
        pprint(avro_reader.writer_schema)


def print_codec():
    with open(avro_file, 'rb') as file_obj:

        avro_reader = fastavro.reader(file_obj)
        print("Avro codec:", avro_reader.codec)


def print_records():
    with open(avro_file, 'rb') as file_obj:
        avro_reader = fastavro.reader(file_obj)

        for record in avro_reader:
            print(record)


def print_records_new_schema():
    new_schema = {
        'fields': [
            {
                'default': None,
                'name': 'client',
                'type': ['null', 'string']
            },
            {
                'default': None,
                'name': 'purchase_date',
                'type': ['null', {'logicalType': 'date', 'type': 'int'}]
            },
            {
                'default': None,
                'name': 'product',
                'type': ['null', 'string']
            },
            {
                'default': None,
                'name': 'price',
                'type': ['null', 'long']
            },
            # {
            #     'default': None,
            #     'name': 'new_field',
            #     'type': ['null', 'string']
            # },
        ],
        'name': 'Root',
        'type': 'record',
    }

    with open(avro_file, 'rb') as file_obj:
        avro_reader = fastavro.reader(file_obj, reader_schema=new_schema)

        for record in avro_reader:
            print(record)


def print_records_by_blocks():
    with open(avro_file, 'rb') as file_obj:
        avro_reader = fastavro.block_reader(file_obj)

        for block_count, block in enumerate(avro_reader):
            print()
            print(f"Start processing block #{block_count}")
            print(f"Num of records in block: {block.num_records}")

            for record in block:
                print(record)

        print()
        print(f"Total avro blocked processed: {block_count + 1}")


if __name__ == '__main__':
    print_schema()
    # print_codec()
    # print_records()
    # print_records_new_schema()
    # print_records_by_blocks()

