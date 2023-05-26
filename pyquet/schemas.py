import os.path

from pyquet import common


class Reader:
    def __init__(self, arg):
        self.schemas = {}
        if isinstance(arg, str):
            if os.path.isdir(arg):
                print("Reading schemas in directory:", arg)
                schemas = common.list_files(arg, regex=".*.json")
                if schemas:
                    for schema in schemas:
                        self.schemas[schema] = common.read_json(schema)
                else:
                    print("No schemas found in directory:", arg)
            elif os.path.isfile(arg):
                if arg.endswith(".json"):
                    print("Reading schema:", arg)
                    self.schemas[arg] = common.read_json(arg)
                else:
                    print(arg, "is not a .json file.")
        elif isinstance(arg, list):
            print("Reading schemas:", ", ".join(arg))
            for schema in arg:
                self.schemas[schema] = common.read_json(schema)
        else:
            print("Invalid input type.", arg)
