"""
This module contains the TableEditor class, which is used to edit the data in a parquet file.
"""

import copy
import shutil
import traceback

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class FieldNotFoundException(Exception):
    """
    Exception raised when a field is not found in the schema.
    """
    def __init__(self, field):
        self.field = field
        self.message = f"Field '{field}' not found in the schema."
        super().__init__(self.message)


class TableEditor:
    """
    A class used to edit the data in a parquet file.
    """
    def __init__(self, path, schema, partitions):
        self.path = path
        self.df = pd.read_parquet(self.path)
        self.schema = schema
        self.tmp_path = path + "_tmp"
        self.partitions = partitions

    def backup_table(self):
        """
        Backup the table in a temporary directory.
        """
        shutil.copytree(self.path, self.tmp_path)

    def set_constants(self, map_dict):
        """
        Set the constants in the table.
        :param map_dict:
        :return: Updated DataFrame
        """
        self.backup_table()
        df = copy.deepcopy(self.df)
        try:
            for column, value in map_dict.items():
                if column not in self.df.columns:
                    raise FieldNotFoundException(column)
                df[column] = value
            table = pa.Table.from_pandas(df)
            table = table.cast(self.schema)
            shutil.rmtree(self.path)
            pq.write_to_dataset(table, self.path, partition_cols=self.partitions)
            self.df = df
        except FieldNotFoundException:
            shutil.rmtree(self.path, ignore_errors=True)
            shutil.copytree(self.tmp_path, self.path)
            traceback.print_exc()
        except Exception as e: # pylint: disable=broad-except
            shutil.rmtree(self.path, ignore_errors=True)
            shutil.copytree(self.tmp_path, self.path)
            print("An error occurred:", e)
            traceback.print_exc()
        finally:
            shutil.rmtree(self.tmp_path)
        return self.df
