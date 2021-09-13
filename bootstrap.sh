#!/bin/bash

bucket_name="$1"

sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install findspark
aws s3 cp s3://sparkify-etl-code-df/ /home/hadoop/ --recursive
cp /home/hadoop/dl-template.cfg /home/hadoop/dl.cfg

#Populate dl.cfg
sed -i "s/S3_BUCKET_NAME=/S3_BUCKET_NAME=${bucket_name}/" dl.cfg
sed -i "s|S3_DESTINATION=s3://|S3_DESTINATION=s3://${bucket_name}/|" dl.cfg

source /home/hadoop/.bashrc
