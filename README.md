# Spark Learning

Current Stacks:

- Spark 2.4 Standalone
- Spark 3.0 Standalone
- Others maybe broken

- Each stack folder contains docker compose for building spark multi-worker spark stacks

## Starting the stack

```Bash

docker-compose -f stacks/pure_spark/docker-compose.yml up --scale spark-worker=3

```

## Using docker spark-submit

```Bash

#Creating some variables to make the docker run command more readable
#App jar environment used by the spark-submit image
SPARK_APPLICATION_JAR_LOCATION="/opt/spark-apps/crimes-app.jar"
#App main class environment used by the spark-submit image
SPARK_APPLICATION_MAIN_CLASS="org.mvb.applications.CrimesApp"
#Extra submit args used by the spark-submit image
SPARK_SUBMIT_ARGS="--conf spark.executor.extraJavaOptions='-Dconfig-path=/opt/spark-apps/dev/config.conf'"

#We have to use the same network as the spark cluster(internally the image resolves spark master as spark://spark-master:7077)
docker run --network docker-spark-cluster_spark-network \
-v /mnt/spark-apps:/opt/spark-apps \
--env SPARK_APPLICATION_JAR_LOCATION=$SPARK_APPLICATION_JAR_LOCATION \
--env SPARK_APPLICATION_MAIN_CLASS=$SPARK_APPLICATION_MAIN_CLASS \
spark-submit:2.3.1

```

## Notebook Spark Submit

```Bash

spark-submit \
--class "SimpleApp" \
--master spark://spark-master:7077 \
target/scala-2.12/simple-project_2.12-1.0.jar
```


### Python lib handling

Need to set the `PYSPARK_DRIVER_PYTHON` path to the conda env used to package the environment
Need to set the `PYSPARK_PYTHON` path to the where the conda env is in the worker envs
We do not use `--archive` as that is for YARN.

Steps

* create conda env for script
* use conda pack to pack up the script
* stash the conda pack .tar.gz in the mounted apps folder and put that path in `PYSPARK_PYTHON`
* put the directory path from the notebook node to the conda env for `PYSPARK_DRIVER_PYTHON`

```Bash
PYSPARK_DRIVER_PYTHON='/opt/conda/envs/test_App/bin/python' \
PYSPARK_PYTHON='/opt/spark-apps/test_App/environment/bin/python' \
spark-submit \
--master spark://spark-master:7077 \
src/main/python/test_script.py

```

TODO:

- pyspark distributing tasks
