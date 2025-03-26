"""
Common functions used in the project.
"""

import json
import os
import re


def read_json(path):
    """
    Read a JSON file.
    :param path: The path to the file
    :return:  content
    :rtype: dict
    """
    with open(path, 'r', encoding='utf-8') as json_file:
        file = json.load(json_file)
    return file


def list_files(path, regex=None):
    """
    List all files in a directory recursively.
    :param path: The path to the directory
    :param regex: A regex to filter the files
    :return: A list with the files
    :rtype: list
    """
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
