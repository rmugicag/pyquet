import os
import unittest
import json
import tempfile
from pyquet.modules.schemas import Reader

class ReaderTestCase(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.schema_1 = os.path.join(self.test_dir.name, 'schema_1.json')
        self.schema_2 = os.path.join(self.test_dir.name, 'schema_2.json')
        self.schema_3 = os.path.join(self.test_dir.name, 'schema_3.json')
        self.bad_schema = os.path.join(self.test_dir.name, 'not_schema.txt')
        self.schemas_list = [self.schema_1, self.schema_2, self.schema_3]

        self.schemas_data = [
            (self.schema_1, {"test": "data1"}),
            (self.schema_2, {"test": "data2"}),
            (self.schema_3, {"test": "data3"})
        ]

        for path, data in self.schemas_data:
            with open(path, 'w') as f:
                json.dump(data, f)

        with open(self.bad_schema, 'w') as f:
            f.write("This is not a JSON schema")

    def tearDown(self):
        self.test_dir.cleanup()

    def test_single_file(self):
        self.reader = Reader(self.schema_1)
        self.assertDictEqual(self.reader.schemas_dict[self.schema_1], {"test": "data1"})

    def test_directory(self):
        self.reader = Reader(self.test_dir.name)
        for path, data in self.schemas_data:
            self.assertDictEqual(self.reader.schemas_dict[path], data)

    def test_directory_without_schemas(self):
        empty_dir = os.path.join(self.test_dir.name, 'empty')
        os.makedirs(empty_dir)
        self.reader = Reader(empty_dir)
        self.assertDictEqual(self.reader.schemas_dict, {})

    def test_array(self):
        self.reader = Reader(self.schemas_list)
        for path, data in self.schemas_data:
            self.assertDictEqual(self.reader.schemas_dict[path], data)

    def test_invalid_file(self):
        self.reader = Reader(self.bad_schema)
        self.assertDictEqual(self.reader.schemas_dict, {})

    def test_invalid_input_type(self):
        self.reader = Reader(123)
        self.assertDictEqual(self.reader.schemas_dict, {})

    def test_mixed_valid_invalid_files(self):
        mixed_list = self.schemas_list + [self.bad_schema]
        self.reader = Reader(mixed_list)
        for path, data in self.schemas_data:
            self.assertDictEqual(self.reader.schemas_dict[path], data)
        self.assertNotIn(self.bad_schema, self.reader.schemas_dict)

if __name__ == '__main__':
    with open('test-reports/results.xml', 'wb') as output:
        unittest.main(verbosity=2)