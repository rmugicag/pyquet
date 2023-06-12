import os.path
import shutil
from datetime import date

import pandas as pd
import tkinter as tk

import traceback
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
from tkinter import filedialog
from pandastable import Table, TableModel


class CustomTable(Table):
    def __init__(self, parent=None, **kwargs):
        Table.__init__(self, parent, **kwargs)
        self.partitions = []
        self.pa_schema = None
        self.file_path = None
        self.schema = None

    def load_csv(self):
        self.file_path = filedialog.askdirectory()
        if self.file_path:
            try:
                df = pd.read_parquet(self.file_path, dtype_backend="pyarrow")
                self.schema = df.dtypes
                for name, field in self.schema.items():
                    if isinstance(field.pyarrow_dtype, pa.DictionaryType):
                        self.partitions.append(name)
                        self.schema[name] = pa.string()
                self.pa_schema = pa.Schema.from_pandas(df)
                for partition in self.partitions:
                    self.schema[partition] = pa.string()
                df_string = pd.read_parquet(self.file_path)
                self.updateModel(TableModel(df_string))
            except pd.errors.ParserError:
                print("Invalid file.")
            except Exception as e:
                print("An error occurred:", e)
                print(traceback.print_exc())

    def save_csv(self):
        if self.file_path:
            file_path = filedialog.askdirectory()
        else:
            file_path = filedialog.askdirectory()
        if file_path:
            try:
                if os.path.exists(file_path):
                    shutil.rmtree(file_path)
                df = self.model.df
                pa_table = pa.Table.from_pandas(df)
                pa_table = pa_table.cast(self.pa_schema)
                pq.write_to_dataset(pa_table, file_path, partition_cols=self.partitions)
            except Exception as e:
                print("An error occurred:", e)
                print(traceback.print_exc())


class CustomToolbar(tk.Frame):
    def __init__(self, parent=None, parent_table=None, **kwargs):
        tk.Frame.__init__(self, parent, **kwargs)
        self.table = parent_table
        self.load_button = tk.Button(self, text="Load Table", command=self.load_csv)
        self.load_button.pack(side=tk.LEFT)
        self.save_button = tk.Button(self, text="Save Table", command=self.save_csv)
        self.save_button.pack(side=tk.LEFT)

    def load_csv(self):
        self.table.load_csv()
        self.table.redraw()

    def save_csv(self):
        self.table.save_csv()


if __name__ == "__main__":
    root = tk.Tk()

    table_frame = tk.Frame(root)
    table_frame.pack(fill=tk.BOTH, expand=True)

    table = CustomTable(table_frame)
    table.show()

    toolbar = CustomToolbar(root, parent_table=table)
    toolbar.pack(side=tk.TOP, fill=tk.X)

    root.mainloop()
