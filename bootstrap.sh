#!/bin/bash

sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install findspark
aws s3 cp s3://sparkify-etl-code-df/ /home/hadoop/ --recursive
cp /home/hadoop/dl-template.cfg /home/hadoop/dl.cfg

source /home/hadoop/.bashrc
