#!/bin/bash

rm -rf /opt/spark-data/libs/environment

mkdir /opt/spark-data/libs/environment

tar -zxf ./conda/environment.tar.gz --directory /opt/spark-data/libs/environment

#conda pack -o conda/environment.tar.gz

#conda pack -p /opt/spark-data/libs/environment