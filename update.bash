#!/usr/bin/env bash

python3 setup.py bdist
python3 setup.py sdist
python3 setup.py install
python3 setup.py build
