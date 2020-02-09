from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setAppName('spark-yarn')
sc = SparkContext(conf=conf)

def some_function(x):
    # Packages are imported and available from your bundled environment.
    #import sklearn
    import pandas
    import sklearn
    import numpy as np

    # Use the libraries to do work
    return np.sin(x)**2 + 2

rdd = (sc.parallelize(range(1000))
         .map(some_function)
         .take(10))

print(rdd)