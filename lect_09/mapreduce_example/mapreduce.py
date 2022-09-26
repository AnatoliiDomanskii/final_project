"""
MapReduce example in vanilla Python.

Problem:
Find max TV price in sales data for 2022-08-01 and 2022-08-01.
Take data from the folders in
lect_06/data_lake/landing/src1/sales/
"""
import uuid
import os
import csv
import json
from typing import Dict, List


CURR_DIR = os.path.dirname(
    os.path.realpath(__file__)
)

DATA_DIR = os.path.join(
    CURR_DIR, 'data'
)

ROOT_DIR = os.path.dirname(
    os.path.dirname(
        CURR_DIR
    )
)

SALES_DATA_DIR = os.path.join(
    ROOT_DIR, 'lect_06/data_lake/landing/src1/sales'
)

STAGE1_DIR = os.path.join(DATA_DIR, 'stage1')
STAGE2_DIR = os.path.join(DATA_DIR, 'stage2')
STAGE3_DIR = os.path.join(DATA_DIR, 'stage3')
STAGE4_DIR = os.path.join(DATA_DIR, 'stage4')
STAGE5_DIR = os.path.join(DATA_DIR, 'stage5')


def _build_random_str() -> str:
    """
    Produce random strings like "4f69485b"
    """
    return str(uuid.uuid4()).split('-')[0]


def mapper(input_rows: List[Dict[str, str]]) -> List[Dict[str, int]]:
    res = []
    for row in input_rows:
        if row['product'] == 'TV':
            price = row['price']
            res.append({'TV': int(price)})
    return res


def reducer1(input_data: List[Dict[str, int]]) -> Dict[str, List[int]]:
    res = []
    for elem in input_data:
        price = elem['TV']
        res.append(price)
    return {'TV': res}


def reducer2(input_data: Dict[str, List[int]]) -> Dict[str, int]:
    prices = input_data['TV']
    return {"TV": max(prices)}


def stage1():
    files = (
        '2022-08-01/src1_sales_2022-08-01__01.csv',
        '2022-08-02/src1_sales_2022-08-02__01.csv',
        '2022-08-02/src1_sales_2022-08-02__01.csv',
    )

    file_paths = [
        os.path.join(SALES_DATA_DIR, file) for file in files
    ]

    for file_path in file_paths:
        stage_file_name = os.path.join(
            STAGE1_DIR,
            _build_random_str() + '.json'
        )

        with open(file_path) as src_file_obj, \
                open(stage_file_name, 'w') as dst_file_obj:
            reader = csv.DictReader(src_file_obj)
            res = mapper(input_rows=[row for row in reader])
            dst_file_obj.write(json.dumps(res))


def stage2():
    file_paths = [
        os.path.join(STAGE1_DIR, file)
        for file in os.listdir(STAGE1_DIR)
    ]

    for file_path in file_paths:
        stage_file_name = os.path.join(
            STAGE2_DIR,
            _build_random_str() + '.json'
        )

        with open(file_path) as src_file_obj, \
                open(stage_file_name, 'w') as dst_file_obj:
            input_data = json.loads(src_file_obj.read())
            res = reducer1(input_data)
            dst_file_obj.write(json.dumps(res))


def stage3():
    file_paths = [
        os.path.join(STAGE2_DIR, file)
        for file in os.listdir(STAGE2_DIR)
    ]

    for file_path in file_paths:
        stage_file_name = os.path.join(
            STAGE3_DIR,
            _build_random_str() + '.json'
        )

        with open(file_path) as src_file_obj, \
                open(stage_file_name, 'w') as dst_file_obj:
            input_data = json.loads(src_file_obj.read())
            res = reducer2(input_data)
            dst_file_obj.write(json.dumps(res))


def stage4():
    file_paths = [
        os.path.join(STAGE3_DIR, file)
        for file in os.listdir(STAGE3_DIR)
    ]

    stage_file_name = os.path.join(
        STAGE4_DIR,
        _build_random_str() + '.json'
    )

    input_data = []

    for file_path in file_paths:
        with open(file_path) as src_file_obj:
            input_data.append(
                json.loads(src_file_obj.read())
            )

    res = reducer1(input_data)
    with open(stage_file_name, 'w') as dst_file_obj:
        dst_file_obj.write(
            json.dumps(res)
        )


def stage5():
    file_path = os.path.join(
        STAGE4_DIR, os.listdir(STAGE4_DIR)[0]
    )

    stage_file_name = os.path.join(
        STAGE5_DIR,
        _build_random_str() + '.json'
    )

    with open(file_path) as src_file_obj:
        input_data = json.loads(src_file_obj.read())

    res = reducer2(input_data)
    print(res)

    with open(stage_file_name, 'w') as dst_file_obj:
        dst_file_obj.write(
            json.dumps(res)
        )


if __name__ == '__main__':
    stage1()
    # stage2()
    # stage3()
    # stage4()
    # stage5()
