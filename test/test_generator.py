import json
import unittest
import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from decimal import Decimal
from pyquet.modules.generator import DataGenerator


class TestDataGenerator(unittest.TestCase):

    def setUp(self):
        self.generator = DataGenerator()
        self.schema = {
            "physicalPath": "/test/path",
            "fields": [
                {"name": "field1", "logicalFormat": "ALPHANUMERIC(10)"},
                {"name": "field2", "logicalFormat": "NUMERIC SHORT"},
                {"name": "field3", "logicalFormat": "DECIMAL(10,2)"},
                {"name": "field4", "logicalFormat": "DATE"},
                {"name": "field5", "logicalFormat": "TIMESTAMP"},
                {"name": "field6", "logicalFormat": "TIME"}
            ]
        }
        self.schema_path = "test_schema.json"
        self.destination_dir = "test_output"
        self.output_type = "csv"

        with open(self.schema_path, 'w') as f:
            json.dump(self.schema, f)

    def tearDown(self):
        if os.path.exists(self.schema_path):
            os.remove(self.schema_path)
        if os.path.exists(self.destination_dir):
            shutil.rmtree(self.destination_dir)

    def test_generate_data_csv(self):
        destination_path, target_schema = self.generator.generate_data(
            self.schema_path, self.output_type, destination_dir=self.destination_dir
        )
        self.assertTrue(os.path.exists(destination_path))
        df = pd.read_csv(destination_path)
        self.assertEqual(len(df), self.generator.num_rows)
        self.assertIn("field1", df.columns)
        self.assertIn("field2", df.columns)
        self.assertIn("field3", df.columns)
        self.assertIn("field4", df.columns)
        self.assertIn("field5", df.columns)
        self.assertIn("field6", df.columns)

    def test_generate_data_parquet(self):
        self.output_type = "parquet"
        destination_path, target_schema = self.generator.generate_data(
            self.schema_path, self.output_type, destination_dir=self.destination_dir
        )
        self.assertTrue(os.path.exists(destination_path))
        table = pq.read_table(destination_path)
        self.assertEqual(table.num_rows, self.generator.num_rows)
        self.assertIn("field1", table.column_names)
        self.assertIn("field2", table.column_names)
        self.assertIn("field3", table.column_names)
        self.assertIn("field4", table.column_names)
        self.assertIn("field5", table.column_names)
        self.assertIn("field6", table.column_names)

    def test_generate_alphanumeric(self):
        data = self.generator.generate_alphanumeric("field1", 10)
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertEqual(len(value), 10)

    def test_generate_int(self):
        data = self.generator.generate_int("field2")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, int)

    def test_generate_decimal(self):
        data = self.generator.generate_decimal("field3", 10, 2)
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, Decimal)

    def test_generate_date(self):
        data = self.generator.generate_date("field4")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)

    def test_generate_timestamp(self):
        data = self.generator.generate_timestamp("field5")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)

    def test_generate_time(self):
        data = self.generator.generate_time("field6")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)


if __name__ == '__main__':
    with open('test-reports/results.xml', 'wb') as output:
        unittest.main(verbosity=2)
