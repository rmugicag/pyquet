"""
This module contains the DataGenerator class, which is used to generate data in various formats.
"""

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
    """
    Class for generating data.
    """

    def __init__(self, catalog_path=None, num_rows=10, limit_rows=True):
        """
        Initialize the DataGenerator with the catalog path and number of rows.

        :param catalog_path: Path to the catalog file, defaults to None
        :type catalog_path: str, optional
        :param num_rows: Number of rows to generate, defaults to 10
        :type num_rows: int, optional
        :param limit_rows: Whether to limit rows based on catalog, defaults to True
        :type limit_rows: bool, optional
        """
        if catalog_path:
            self.catalog = common.read_json(catalog_path)
            if limit_rows:
                self.num_rows = len(max(self.catalog.values(), key=len))
            else:
                self.num_rows = num_rows
        else:
            self.catalog = {}
            self.num_rows = num_rows

    def generate_data(self,
                      schema_path,
                      output_type,
                      partitions=None,
                      destination_path=None,
                      destination_dir=None):
        """
        Generates data based on the schema and saves it to the specified location.

        :param schema_path: Path to the schema file
        :type schema_path: str
        :param output_type: Type of output (e.g., 'csv', 'parquet')
        :type output_type: str
        :param partitions: List of partition columns, defaults to None
        :type partitions: list, optional
        :param destination_path: Path to save the generated data, defaults to None
        :type destination_path: str, optional
        :param destination_dir: Directory to save the generated data, defaults to None
        :type destination_dir: str, optional
        :return: Tuple of target path and target schema
        :rtype: tuple
        """
        data = {}
        field_format = []
        schema = common.read_json(schema_path)
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
                field_format.append((field["name"],
                                     pa.decimal128(decimal_precision, decimal_scale)))
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
            target_path = destination_path
        elif destination_dir:
            target_path = os.path.join(destination_dir, schema["name"])
        else:
            target_path = schema["physicalPath"]

        target_dir = os.path.dirname(target_path)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        print("Writing data in:", target_path)

        if os.path.isfile(target_path):
            os.remove(target_path)
        df = pd.DataFrame(data)
        target_schema = pa.schema(field_format)
        if output_type == "csv":
            if not target_path.endswith(".csv"):
                target_path += ".csv"
            df.to_csv(target_path, index=False)
        elif output_type == "parquet":
            table = pa.Table.from_pandas(df)
            table = table.cast(target_schema)
            pq.write_to_dataset(table, target_path, partition_cols=partitions)
        else:
            print("Unrecognized output type:", output_type, "Valid options are: csv, parquet")
        return target_path, target_schema

    def generate_alphanumeric(self, name, size=1):
        """
        Generates alphanumeric data.

        :param name: Name of the field
        :type name: str
        :param size: Length of the alphanumeric string, defaults to 1
        :type size: int, optional
        :return: List of alphanumeric strings
        :rtype: list
        """
        alphanumeric_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = "".join(random.choices(string.ascii_letters + string.digits, k=size))
            alphanumeric_list.append(value)
        return alphanumeric_list

    def generate_int(self, name, size=1000):
        """
        Generates integer data.

        :param name: Name of the field
        :type name: str
        :param size: Maximum value of the integer, defaults to 1000
        :type size: int, optional
        :return: List of integers
        :rtype: list
        """
        int_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = random.randint(0, size)
            int_list.append(value)
        return int_list

    def generate_decimal(self, name, decimal_precision=12, decimal_scale=6):
        """
        Generates decimal data.

        :param name: Name of the field
        :type name: str
        :param decimal_precision: Precision of the decimal, defaults to 12
        :type decimal_precision: int, optional
        :param decimal_scale: Scale of the decimal, defaults to 6
        :type decimal_scale: int, optional
        :return: List of decimals
        :rtype: list
        """
        decimal_list = []
        for counter in range(self.num_rows):
            if name in self.catalog:
                value = self.catalog[name][counter % len(self.catalog[name])]
            else:
                value = Decimal(random.randrange(10 ** decimal_precision)) / (10 ** decimal_scale)
            decimal_list.append(value)
        return decimal_list

    def generate_date(self, name, date_format='%Y-%m-%d'):
        """
        Generates date data.

        :param name: Name of the field
        :type name: str
        :param date_format: Format of the date, defaults to '%Y-%m-%d'
        :type date_format: str, optional
        :return: List of dates
        :rtype: list
        """
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
        """
        Generates timestamp data.

        :param name: Name of the field
        :type name: str
        :param date_format: Format of the timestamp, defaults to '%Y-%m-%d %H:%M:%S'
        :type date_format: str, optional
        :return: List of timestamps
        :rtype: list
        """
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
        timestamp_list = pd.Series(timestamp_list).apply(lambda x: datetime.strptime(x,
                                                                                     date_format))
        return timestamp_list

    def generate_time(self, name, date_format='%H:%M:%S'):
        """
        Generates time data.

        :param name: Name of the field
        :type name: str
        :param date_format: Format of the time, defaults to '%H:%M:%S'
        :type date_format: str, optional
        :return: List of times
        :rtype: list
        """
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
