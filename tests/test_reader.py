import os
import unittest
from pyquet.schemas import Reader


class ReaderTestCase(unittest.TestCase):
    def setUp(self):
        self.schemas_path = os.path.join(os.path.dirname(__file__), "test_data", "schemas")
        self.schemas_path_without_schemas = os.path.join(self.schemas_path, "data")
        self.schema_1 = os.path.join(self.schemas_path, 'schema_1.json')
        self.schema_2 = os.path.join(self.schemas_path, 'schema_2.json')
        self.schema_3 = os.path.join(self.schemas_path, 'schema_3.json')
        self.schemas_list = [self.schema_1, self.schema_2, self.schema_3]
        self.bad_schema = os.path.join(self.schemas_path, 'not_schema.txt')

    def test_single_file(self):
        self.reader = Reader(os.path.join(self.schema_1))
        self.assertDictEqual(self.reader.schemas_dict[self.schema_1], {"test": "data1"})

    def test_directory(self):
        self.reader = Reader(self.schemas_path)
        self.assertDictEqual(self.reader.schemas_dict[self.schema_1], {"test": "data1"})
        self.assertDictEqual(self.reader.schemas_dict[self.schema_2], {"test": "data2"})
        self.assertDictEqual(self.reader.schemas_dict[self.schema_3], {"test": "data3"})

    def test_directory_without_schemas(self):
        self.reader = Reader(self.schemas_path_without_schemas)
        self.assertDictEqual(self.reader.schemas_dict, {})

    def test_array(self):
        self.reader = Reader(self.schemas_list)
        self.assertDictEqual(self.reader.schemas_dict[self.schema_1], {"test": "data1"})
        self.assertDictEqual(self.reader.schemas_dict[self.schema_2], {"test": "data2"})
        self.assertDictEqual(self.reader.schemas_dict[self.schema_3], {"test": "data3"})

    def test_invalid_file(self):
        self.reader = Reader(self.bad_schema)
        self.assertDictEqual(self.reader.schemas_dict, {})

    def test_invalid_input_type(self):
        self.reader = Reader(123)
        self.assertDictEqual(self.reader.schemas_dict, {})


if __name__ == '__main__':
    unittest.main()
