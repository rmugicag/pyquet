import json
import unittest
import os
import shutil
import pandas as pd
from datetime import datetime
from decimal import Decimal
from pyquet.modules.generator import DataGenerator


class TestDataGenerator(unittest.TestCase):

    def setUp(self):
        self.tests_path = "tests_path"
        self.generator = DataGenerator()
        self.schema = {
            "name": "test_table",
            "physicalPath": os.path.join(self.tests_path, "schema_physical_path"),
            "fields": [
                {"name": "alphanumeric_field", "logicalFormat": "ALPHANUMERIC(10)"},
                {"name": "numeric_short_field", "logicalFormat": "NUMERIC SHORT"},
                {"name": "decimal_field", "logicalFormat": "DECIMAL(10,2)"},
                {"name": "date_field", "logicalFormat": "DATE"},
                {"name": "timestamp_field", "logicalFormat": "TIMESTAMP"},
                {"name": "time_field", "logicalFormat": "TIME"}
            ]
        }
        self.schema_path = os.path.join(self.tests_path, "test_schema.json")
        self.destination_dir = os.path.join(self.tests_path, "destination_dir")
        self.destination_path = os.path.join(self.tests_path, "destination_path")

        os.makedirs(os.path.dirname(self.schema_path), exist_ok=True)

        with open(self.schema_path, "w") as f:
            json.dump(self.schema, f)

    def tearDown(self):
        if os.path.exists(self.tests_path):
            shutil.rmtree(self.tests_path)

    def test_generate_data(self):

        # Generate csv and parquet data and save it to files without destination path nor destination dir.
        self.generator.generate_data(self.schema_path, output_type="csv")
        self.generator.generate_data(self.schema_path, output_type="parquet")
        self.assertTrue(os.path.exists(self.schema["physicalPath"] + ".csv"))
        self.assertTrue(os.path.exists(self.schema["physicalPath"]))
        df_csv1 = pd.read_csv(self.schema["physicalPath"] + ".csv")
        df_parquet1 = pd.read_parquet(self.schema["physicalPath"])

        # Generate csv and parquet data and save it to files with destination path.
        self.generator.generate_data(self.schema_path, output_type="csv", destination_path=self.destination_path)
        self.generator.generate_data(self.schema_path, output_type="parquet", destination_path=self.destination_path)
        self.assertTrue(os.path.exists(self.destination_path + ".csv"))
        self.assertTrue(os.path.exists(self.destination_path))
        df_csv2 = pd.read_csv(self.destination_path + ".csv")
        df_parquet2 = pd.read_parquet(self.destination_path)

        # Generate csv and parquet data and save it to files with destination dir.
        self.generator.generate_data(self.schema_path, output_type="csv", destination_dir=self.destination_dir)
        self.generator.generate_data(self.schema_path, output_type="parquet", destination_dir=self.destination_dir)
        self.assertTrue(os.path.exists(os.path.join(self.destination_dir, self.schema["name"] + ".csv")))
        self.assertTrue(os.path.exists(os.path.join(self.destination_dir, self.schema["name"])))
        df_csv3 = pd.read_csv(os.path.join(self.destination_dir, self.schema["name"] + ".csv"))
        df_parquet3 = pd.read_parquet(os.path.join(self.destination_dir, self.schema["name"]))

        # Check if the columns are the same in all files.
        self.assertListEqual(list(df_csv1.columns), list(df_csv2.columns))
        self.assertListEqual(list(df_csv1.columns), list(df_csv3.columns))
        self.assertListEqual(list(df_csv1.columns), list(df_parquet2.columns))
        self.assertListEqual(list(df_parquet1.columns), list(df_parquet2.columns))
        self.assertListEqual(list(df_parquet1.columns), list(df_parquet3.columns))

        # Check if the number of rows is the same in all files.
        self.assertEqual(len(df_csv1), self.generator.num_rows)

        # Check if the columns are the schema fields.
        self.assertIn("alphanumeric_field", df_csv1.columns)
        self.assertIn("numeric_short_field", df_csv1.columns)
        self.assertIn("decimal_field", df_csv1.columns)
        self.assertIn("date_field", df_csv1.columns)
        self.assertIn("timestamp_field", df_csv1.columns)
        self.assertIn("time_field", df_csv1.columns)


    def test_generate_alphanumeric(self):
        data = self.generator.generate_alphanumeric("alphanumeric_field", 10)
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertEqual(len(value), 10)

    def test_generate_int(self):
        data = self.generator.generate_int("numeric_short_field")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, int)

    def test_generate_decimal(self):
        data = self.generator.generate_decimal("decimal_field", 10, 2)
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, Decimal)

    def test_generate_date(self):
        data = self.generator.generate_date("date_field")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)

    def test_generate_timestamp(self):
        data = self.generator.generate_timestamp("timestamp_field")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)

    def test_generate_time(self):
        data = self.generator.generate_time("time_field")
        self.assertEqual(len(data), self.generator.num_rows)
        for value in data:
            self.assertIsInstance(value, datetime)


if __name__ == '__main__':
    unittest.main()
