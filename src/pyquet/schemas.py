import os.path
import re
import pandas as pd
from . import common


class Reader:
    def __init__(self, arg, regex=".*.json"):
        self.schemas_dict = {}
        print(arg)
        if isinstance(arg, str):
            if os.path.isdir(arg):
                print("Reading schemas in directory:", arg)
                schemas = common.list_files(arg, regex)
                print(schemas)
                if schemas:
                    for schema in schemas:
                        self.schemas_dict[schema] = common.read_json(schema)
                else:
                    print("No schemas found in directory:", arg)
            elif os.path.isfile(arg):
                if re.match(regex, arg):
                    print("Reading schema:", arg)
                    self.schemas_dict[arg] = common.read_json(arg)
                else:
                    print(arg, "is not a .json file.")
        elif isinstance(arg, list):
            print("Reading schemas:", ", ".join(arg))
            for schema in arg:
                self.schemas_dict[schema] = common.read_json(schema)
        else:
            print("Invalid input type.", arg)
        self.schemas_df = pd.DataFrame.from_dict(self.schemas_dict, orient="index")
        self.schemas_df = self.schemas_df.fillna("")
        self.schemas_dict = self.schemas_df.to_dict(orient="index")
        self.unique_fields = self.__get_unique_fields()
        self.unique_partitions = self.__get_unique_partitions()
        self.unique_data_types = self.__get_unique_data_types()

    def __get_unique_fields(self):
        return list({field["name"] for schema in self.schemas_dict.values() if "fields" in schema for field in
                     schema["fields"]})

    def __get_unique_partitions(self):
        return list({partition for schema in self.schemas_dict.values() if "partitions" in schema for partition in
                     schema["partitions"]})

    def __get_unique_data_types(self):
        return list({field["logicalFormat"] for schema in self.schemas_dict.values() if "fields" in schema for field in
                     schema["fields"]})
