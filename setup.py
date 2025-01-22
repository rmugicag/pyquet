#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as f:
    readme = f.read()

# Extract the requirements from the deps file.
with open(os.path.join(here, "requirements.txt")) as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

with open(os.path.join(here, "requirements_dev.txt")) as f:
    test_requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='pyquet',
    version='1.0.3',
    description='Generate pseudorandom data from .json schemas',
    author='Ricardo Múgica',
    author_email='rmugicag@gmail.com',
    entry_points={
        "console_scripts": [
            "PyquetGenerate=pyquet.pyquet_generator:main"
        ]
    },
    install_requires=requirements,
    extras_require={
        'dev': test_requirements,
    },
    packages=find_packages(),
)