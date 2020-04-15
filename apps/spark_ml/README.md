# Spark ML

The goal for this app is to run common machine learning tasts

## Setup

- Pure Spark 2.4.3 cluster
- shared folder /opt/spark-data/libs/available on all nodes
  - holds xgboost4j / xgboost4j-spark / pythonwrapper for xgb spark

## Building

sbt assembly is required to build uberjar with sbt

```Bash

sbt clean
sbt compile
sbt package

```

We are using 2.11.12 - later scala needs xgb > 0.9


## spark-submitting

Scala:

```Bash

spark-submit \
--class "TrainingApp" \
--master spark://spark-master:7077 \
target/scala-2.11/spark-ml_2.12-1.0.jar

```

Python:

```Bash

spark-submit \
--master spark://spark-master:7077 \
--jars /opt/spark-data/libs/xgboost4j-spark-0.90.jar,/opt/spark-data/libs/xgboost4j-0.90.jar \
src/main/python/main.py

```

## Adding Python Dependencies

See: https://github.com/Data-drone/spark-submit-deps
for packaging

Note this packaging doesn't work for numpy/pandas/xgb due to numpy compiling with C extensions.
These all need to be installed on nodes

```Bash
# run from python dir

spark-submit \
--master spark://spark-master:7077 \
--py-files spark_dist/skdist_test-0.1-deps.zip,spark_dist/skdist_test-0.1.zip \
skdist_test.py

```

Conda-Pack option

--files only copies files
--archive only works with yarn



```Bash
# run from python dir
# this is with a spark standalone cluster
# we use the shared /opt/spark-data folder
# and we unpacked environment manually into that

PYSPARK_DRIVER_PYTHON=`which python` \
PYSPARK_PYTHON=/opt/spark-data/libs/environment/bin/python \
spark-submit \
--master spark://spark-master:7077 \
skdist_test.py

```