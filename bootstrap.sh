#!/bin/bash

export PYSPARK_PYTHON=python3

sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install findspark
aws s3 cp s3://sparkify-etl-code-df/ . --recursive
cp dl-template.cfg dl.cfg
