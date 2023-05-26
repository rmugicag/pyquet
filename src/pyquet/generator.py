import os.path
import random
import re
import string
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
from src.pyquet import common

import pyarrow as pa
import pyarrow.parquet as pq


def generate_test_table(schema, catalog_path):
    catalog = common.read_json(catalog_path)
    min_rows = len(max(catalog.values(), key=lambda x: len(x)))
    data = {}
    field_format = []
    for field in schema["fields"]:
        name = field["name"]
        data_type = field["logicalFormat"]
        if data_type.startswith("ALPHANUMERIC"):
            match = re.match(r'ALPHANUMERIC\(([0-9]*)\)', data_type)
            string_len = match.group(1)
            data[name] = generate_alphanumeric(min_rows, int(string_len))
            field_format.append((field["name"], pa.string()))
        elif data_type.startswith("NUMERIC SHORT"):
            data[name] = generate_int(min_rows)
            field_format.append((field["name"], pa.int64()))
        elif data_type.startswith("DECIMAL"):
            match = re.match(r'DECIMAL\(([0-9]*),?([0-9]*)\)', data_type)
            decimal_precision = int(match.group(1))
            decimal_scale = match.group(2)
            decimal_scale = int(decimal_scale) if decimal_scale != "" else 0
            data[name] = generate_decimal(min_rows, decimal_precision, decimal_scale)
            field_format.append((field["name"], pa.decimal128(decimal_precision, decimal_scale)))
        elif data_type.startswith("DATE"):
            data[name] = generate_date(min_rows)
            field_format.append((field["name"], pa.date32()))
        elif data_type.startswith("TIMESTAMP"):
            data[name] = generate_timestamp(min_rows)
            field_format.append((field["name"], pa.timestamp("ms")))
        elif data_type.startswith("TIME"):
            data[name] = generate_time(min_rows)
            field_format.append((field["name"], pa.time32("ms")))
        else:
            print("Unrecognized logicalFormat:", data_type)

    write_parquet_table(data, schema["physicalPath"], pa.schema(field_format))


def generate_alphanumeric(num_rows, size=1):
    alphanumeric_list = []
    for _ in range(num_rows):
        value = "".join(random.choices(string.ascii_letters + string.digits, k=size))
        alphanumeric_list.append(value)
    return alphanumeric_list


def generate_int(num_rows, size=1000):
    int_list = []
    for _ in range(num_rows):
        value = random.randint(0, size)
        int_list.append(value)
    return int_list


def generate_decimal(num_rows, decimal_precision=12, decimal_scale=6):
    decimal_list = []
    for _ in range(num_rows):
        value = Decimal(random.randrange(10 ** decimal_precision)) / (10 ** decimal_scale)
        decimal_list.append(value)
    return decimal_list


def generate_date(num_rows, date_format='%Y-%m-%d'):
    date_list = []
    for _ in range(num_rows):
        date = datetime.today() - timedelta(days=random.randint(0, 365 * 5),
                                            hours=random.randint(0, 24),
                                            minutes=random.randint(0, 60),
                                            seconds=random.randint(0, 60))
        date_list.append(str(date.strftime(date_format)))
    date_list = pd.Series(date_list).apply(lambda x: datetime.strptime(x, date_format))
    return date_list


def generate_timestamp(num_rows, date_format='%Y-%m-%d %H:%M:%S'):
    timestamp_list = []
    for _ in range(num_rows):
        date = datetime.today() - timedelta(days=random.randint(0, 365 * 5),
                                            hours=random.randint(0, 24),
                                            minutes=random.randint(0, 60),
                                            seconds=random.randint(0, 60))
        timestamp_list.append(str(date.strftime(date_format)))
    timestamp_list = pd.Series(timestamp_list).apply(lambda x: datetime.strptime(x, date_format))
    return timestamp_list


def generate_time(num_rows, date_format='%H:%M:%S'):
    time_list = []
    for _ in range(num_rows):
        date = datetime.today() - timedelta(days=random.randint(0, 365 * 5),
                                            hours=random.randint(0, 24),
                                            minutes=random.randint(0, 60),
                                            seconds=random.randint(0, 60))
        time_list.append(str(date.strftime(date_format)))
    time_list = pd.Series(time_list).apply(lambda x: datetime.strptime(x, date_format))
    return time_list


def write_parquet_table(data, path, schema, destination_dir="."):
    destination_path = os.path.join(destination_dir, path)
    destination_dir = os.path.dirname(destination_path)
    if not os.path.isdir(destination_dir):
        os.makedirs(destination_dir)
    if os.path.isfile(destination_path):
        os.remove(destination_path)
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    target_schema = pa.schema(schema)
    table = table.cast(target_schema)
    pq.write_table(table, destination_path)

