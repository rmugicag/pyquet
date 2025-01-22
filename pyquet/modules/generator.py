import os.path
import random
import re
import string
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from . import common


class DataGenerator:
    def __init__(self, catalog_path=None, num_rows=10, limit_rows=True):
        if catalog_path:
            self.catalog = common.read_json(catalog_path)
            if limit_rows:
                self.num_rows = len(max(self.catalog.values(), key=lambda x: len(x)))
            else:
                self.num_rows = num_rows
        else:
            self.catalog = {}
            self.num_rows = num_rows

    def generate_data(self, schema_path, output_type, partitions=None, destination_path=None, destination_dir=None):
        data = {}
        field_format = []
        schema = common.read_json(schema_path)
        schema_physical_path = os.path.normpath(schema["physicalPath"]).replace("\\", "/")
        for field in schema["fields"]:
            name = field["name"]
            data_type = field["logicalFormat"]
            if data_type.startswith("ALPHANUMERIC"):
                match = re.match(r'ALPHANUMERIC\(([0-9]*)\)', data_type)
                string_len = match.group(1)
                data[name] = self.generate_alphanumeric(name, int(string_len))
                field_format.append((field["name"], pa.string()))
            elif data_type.startswith("NUMERIC SHORT"):
                data[name] = self.generate_int(name)
                field_format.append((field["name"], pa.int64()))
            elif data_type.startswith("DECIMAL"):
                match = re.match(r'DECIMAL\(([0-9]*),?([0-9]*)\)', data_type)
                decimal_precision = int(match.group(1))
                decimal_scale = match.group(2)
                decimal_scale = int(decimal_scale) if decimal_scale != "" else 0
                data[name] = self.generate_decimal(name, decimal_precision, decimal_scale)
                field_format.append((field["name"], pa.decimal128(decimal_precision, decimal_scale)))
            elif data_type.startswith("DATE"):
                data[name] = self.generate_date(name)
                field_format.append((field["name"], pa.date32()))
            elif data_type.startswith("TIMESTAMP"):
                data[name] = self.generate_timestamp(name)
                field_format.append((field["name"], pa.timestamp("ms")))
            elif data_type.startswith("TIME"):
                data[name] = self.generate_time(name)
                field_format.append((field["name"], pa.time32("ms")))
            else:
                print("Unrecognized logicalFormat:", data_type)

        if destination_path:
            destination_path_dir = os.path.dirname(destination_path)
            if destination_path_dir and not os.path.exists(destination_path_dir):
                os.makedirs(destination_path_dir)
        else:
            if destination_dir:
                destination_path = os.path.join(destination_dir, schema_physical_path[1:] if schema_physical_path.startswith("/") else schema_physical_path)
            else:
                destination_path = schema_physical_path[1:] if schema_physical_path.startswith("/") else schema_physical_path

            destination_path_dir = os.path.dirname(destination_path)
            if not os.path.exists(destination_path_dir):
                os.makedirs(destination_path_dir)

        print("Destination path:", destination_path)

        if os.path.isfile(destination_path):
            os.remove(destination_path)
        df = pd.DataFrame(data)
        target_schema = pa.schema(pa.schema(field_format))
        if output_type == "csv":
            df.to_csv(destination_path, index=False)
        elif output_type == "parquet":
            table = pa.Table.from_pandas(df)
            table = table.cast(target_schema)
            pq.write_to_dataset(table, destination_path, partition_cols=partitions)
        else:
            print("Unrecognized output type:", output_type, "Valid options are: csv, parquet")
        return destination_path, target_schema

    def generate_alphanumeric(self, name, size=1):
        alphanumeric_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = "".join(random.choices(string.ascii_letters + string.digits, k=size))
            alphanumeric_list.append(value)
        return alphanumeric_list

    def generate_int(self, name, size=1000):
        int_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = random.randint(0, size)
            int_list.append(value)
        return int_list

    def generate_decimal(self, name, decimal_precision=12, decimal_scale=6):
        decimal_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = Decimal(random.randrange(10 ** decimal_precision)) / (10 ** decimal_scale)
            decimal_list.append(value)
        return decimal_list

    def generate_date(self, name, date_format='%Y-%m-%d'):
        date_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                date = self.catalog[name][counter % len(self.catalog[name])]
            else:
                date = datetime.today() - timedelta(days=random.randint(0, 365 * 5))
                date = date.strftime(date_format)
            date_list.append(date)
        date_list = pd.Series(date_list).apply(lambda x: datetime.strptime(x, date_format))
        return date_list

    def generate_timestamp(self, name, date_format='%Y-%m-%d %H:%M:%S'):
        timestamp_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                date = self.catalog[name][counter % len(self.catalog[name])]
            else:
                date = datetime.today() - timedelta(days=random.randint(0, 365 * 5),
                                                    hours=random.randint(0, 24),
                                                    minutes=random.randint(0, 60),
                                                    seconds=random.randint(0, 60))
                date = date.strftime(date_format)
            timestamp_list.append(date)
        timestamp_list = pd.Series(timestamp_list).apply(lambda x: datetime.strptime(x, date_format))
        return timestamp_list

    def generate_time(self, name, date_format='%H:%M:%S'):
        time_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                date = self.catalog[name][counter % len(self.catalog[name])]
            else:
                date = datetime.today() - timedelta(hours=random.randint(0, 24),
                                                    minutes=random.randint(0, 60),
                                                    seconds=random.randint(0, 60))
                date = date.strftime(date_format)
            time_list.append(date)
        time_list = pd.Series(time_list).apply(lambda x: datetime.strptime(x, date_format))
        return time_list
