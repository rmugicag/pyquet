import json
import os
import re


def read_json(path):
    with open(path, 'r') as schema_json:
        schema = json.load(schema_json)
    return schema


def list_files(path, regex=None):
    files = []
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            if regex:
                if re.match(regex, file_path):
                    files.append(file_path)
            else:
                files.append(file_path)
        elif os.path.isdir(file_path):
            files.extend(list_files(file_path, regex))
    return files
