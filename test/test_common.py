import unittest
import tempfile
import os
import json
from pyquet.modules.common import read_json, list_files

class TestCommonFunctions(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_file = os.path.join(self.test_dir.name, 'test.json')
        self.test_data = {"key": "value"}
        with open(self.test_file, 'w') as f:
            json.dump(self.test_data, f)

    def tearDown(self):
        # Cleanup the temporary directory
        self.test_dir.cleanup()

    def test_read_json(self):
        # Test reading a JSON file
        result = read_json(self.test_file)
        self.assertEqual(result, self.test_data)

    def test_list_files_no_regex(self):
        # Test listing files without regex
        result = list_files(self.test_dir.name)
        self.assertIn(self.test_file, result)

    def test_list_files_with_regex(self):
        # Test listing files with regex
        result = list_files(self.test_dir.name, r'.*\.json')
        self.assertIn(self.test_file, result)

    def test_list_files_with_regex_no_match(self):
        # Test listing files with regex that does not match
        result = list_files(self.test_dir.name, r'.*\.txt')
        self.assertNotIn(self.test_file, result)
        self.assertEqual(result, [])

if __name__ == '__main__':
    with open('test-reports/results.xml', 'wb') as output:
        unittest.main(verbosity=2)