from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import os

# add jars for xgb
# do in the bash line instead
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /opt/spark-data/libs/xgboost4j-spark-0.90.jar,/opt/spark-data/libs/xgboost4j-0.90.jar pyspark-shell'

# start spark session
spark = SparkSession\
        .builder\
        .appName("PySpark xgb new")\
        .getOrCreate()

# add in pywrapper
spark.sparkContext.addPyFile("/opt/spark-data/libs/pyspark-xgboost.zip")

# need to add in to import
import sparkxgb

# create schema type for dataset
schema = StructType([
            StructField("sepal length", DoubleType(), True),
            StructField("sepal width", DoubleType(), True),
            StructField("petal length", DoubleType(), True),
            StructField("petal width", DoubleType(), True),
            StructField("class", StringType(), True)
        ])

# load in iris dataset
rawInput = spark.read.schema(schema).csv("/opt/spark-data/iris.data")

# convert text class to index
stringIndexer = StringIndexer(inputCol="class", outputCol="classIndex")
model = stringIndexer.fit(rawInput)

# transform labels
labelTransformed = model.transform(rawInput).drop("class")

# xgb spark requires 
vectorAssembler = VectorAssembler(inputCols=["sepal length", "sepal width", "petal length", "petal width"],
                                  outputCol="features")

xgbInput = vectorAssembler.transform(labelTransformed)
xgbInput = xgbInput.select("features", "classIndex")

train, test = xgbInput.randomSplit([0.8,0.2])

xgb_model = sparkxgb.XGBoostClassifier(
    featuresCol="features",
    labelCol="classIndex"
)

xgb_model.setParams(eta=0.1,
                   maxDepth=2,
                   objective="multi:softprob",
                   numClass=3,
                   numRound=10,
                   numWorkers=2)

model = xgb_model.fit(train)

results = model.transform(test)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",
                                              labelCol="classIndex",
                                              metricName="weightedPrecision")

# compute the classification error on test data.
accuracy = evaluator.evaluate(results)
print("Test Error = %g" % (1.0 - accuracy))

spark.stop()