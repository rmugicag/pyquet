import os
import shutil
import unittest

import pandas as pd

from pyquet.pyquet_generator import DataGenerator


class DataGeneratorTestCase(unittest.TestCase):
    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "test_data", "schemas", "t_kcog_entity.output.schema")
        self.catalog_path = os.path.join(os.path.dirname(__file__), "test_data", "catalog.json")
        self.output_dir = os.path.join(os.path.dirname(__file__), "test_data", "output")

    def tearDown(self):
       if os.path.exists(self.output_dir):
           shutil.rmtree(self.output_dir)

    def test_write_csv(self):
        generator = DataGenerator(self.catalog_path)
        generated_data_path, schema = generator.generate_data(self.schema_path, "csv", ["gf_cutoff_date"], self.output_dir)
        generated_df = pd.read_csv(generated_data_path)

        self.assertEqual(len(generated_df), 3)



    def test_write_parquet(self):
        generator = DataGenerator(self.catalog_path)
        generated_data_path, schema = generator.generate_data(self.schema_path, "parquet", ["gf_cutoff_date"], self.output_dir)
        generated_df = pd.read_parquet(generated_data_path)

        root, dir_names, _ = next(os.walk(generated_data_path))

        self.assertEqual(len(dir_names), 3)
        self.assertEqual(len(generated_df), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
