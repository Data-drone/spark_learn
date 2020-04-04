#!/bin/bash

# start from scratch
rm -rf /tmp/hdfs
rm -rf /tmp/mysql
docker-compose up --scale spark-worker=3