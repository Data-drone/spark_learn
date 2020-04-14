name := "Spark ML"

version := "1.0"
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3"

libraryDependencies += "ml.dmlc" %% "xgboost4j-spark" % "0.9" from "file:///opt/spark-data/libs/xgboost4j-0.90.jar"
libraryDependencies += "ml.dmlc" %% "xgboost4j"  % "0.9" from "file:///opt/spark-data/libs/xgboost4j-spark-0.90.jar"