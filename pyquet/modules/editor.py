import copy
import shutil
import traceback

import pandas as pd
import pyarrow as pa

pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class FieldNotFoundException(Exception):
    def __init__(self, field):
        self.field = field
        self.message = f"Field '{field}' not found in the schema."
        super().__init__(self.message)


class TableEditor:
    def __init__(self, path, schema, partitions):
        self.path = path
        self.df = pd.read_parquet(self.path)
        self.schema = schema
        self.tmp_path = path + "_tmp"
        self.partitions = partitions

    def backup_table(self):
        shutil.copytree(self.path, self.tmp_path)

    def set_constants(self, map_dict):
        self.backup_table()
        df = copy.deepcopy(self.df)
        try:
            for column, value in map_dict.items():
                if column not in self.df.columns:
                    raise FieldNotFoundException(column)
                print(type(df[column]))
                df[column] = value
            table = pa.Table.from_pandas(df)
            table = table.cast(self.schema)
            shutil.rmtree(self.path)
            pq.write_to_dataset(df, self.path, partition_cols=self.partitions)
            self.df = df
        except FieldNotFoundException as e:
            shutil.rmtree(self.path, ignore_errors=True)
            shutil.copytree(self.tmp_path, self.path)
            traceback.print_exc()
        except Exception as e:
            shutil.rmtree(self.path, ignore_errors=True)
            shutil.copytree(self.tmp_path, self.path)
            traceback.print_exc()
        finally:
            shutil.rmtree(self.tmp_path)
            return self.df
