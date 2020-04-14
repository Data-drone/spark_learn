/* TrainingApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier


object TrainingApp {
  def main(args: Array[String]) {

    val spark = SparkSession
        .builder
        .appName("Spark XGB")
        .getOrCreate()
    
    val schema = new StructType(Array(
      StructField("sepal length", DoubleType, true),
      StructField("sepal width", DoubleType, true),
      StructField("petal length", DoubleType, true),
      StructField("petal width", DoubleType, true),
      StructField("class", StringType, true)))

    val rawInput = spark.read.schema(schema).csv("/opt/spark-data/iris.data")

    val stringIndexer = new StringIndexer().
      setInputCol("class").
      setOutputCol("classIndex").
      fit(rawInput)

    val labelTransformed = stringIndexer.transform(rawInput).drop("class")

    val vectorAssembler = new VectorAssembler().
      setInputCols(Array("sepal length", "sepal width", "petal length", "petal width")).
      setOutputCol("features")
    
    val xgbInput = vectorAssembler.transform(labelTransformed).select("features", "classIndex")


    val xgbParam = Map("eta" -> 0.1f,
      "max_depth" -> 2,
      "objective" -> "multi:softprob",
      "num_class" -> 3,
      "num_round" -> 100,
      "num_workers" -> 2)

    val xgbClassifier = new XGBoostClassifier(xgbParam).
      setFeaturesCol("features").
      setLabelCol("classIndex")

    val xgbClassificationModel = xgbClassifier.fit(xgbInput)

    spark.stop()
  }
}
