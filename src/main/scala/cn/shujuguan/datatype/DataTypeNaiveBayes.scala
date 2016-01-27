package cn.shujuguan.datatype

import cn.shujuguan.common.{DataType, Util}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yueyang on 1/22/16.
  */
object DataTypeNaiveBayes {

  val sparkConf = new SparkConf()
    .setAppName("Data Type Training")
    .setMaster("local")

  val sparkContext = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sparkContext)

  // Dataset API
  //    val trainingDataset = DataTypeDataset.makeDataset(sqlContext)
  //
  //    val dataFrame = trainingDataset.toDF().map(r => {
  //      (r.getDouble(0), r.getString(1), Util.string2Vector(r.getString(1), 100))
  //    }).toDF("label", "content", "features")

  val trainingRDD = DataTypeRDD.makeRDD(sparkContext)

  val dataFrame = sqlContext.createDataFrame(trainingRDD.map(d => {
    (d.label, d.content, Util.string2Vector(d.content, 100))
  })).toDF("label", "content", "features")

  val splitsDataFrame = dataFrame.randomSplit(Array(0.75, 0.25))

  val trainingDataFrame = splitsDataFrame(0)
  val testDataFrame = splitsDataFrame(1)

  val model = train(trainingDataFrame)

  def main(args: Array[String]) {
    test(testDataFrame)
  }

  def test(testDataFrame: DataFrame): Unit = {

    val predictions = model.transform(testDataFrame)

    val multiclassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val accuracy = multiclassClassificationEvaluator.evaluate(predictions)

    val predictionAndLabel = predictions.select("prediction", "label").map(r => (r.getDouble(0), r.getDouble(1)))
    val metrics = new MulticlassMetrics(predictionAndLabel)

    val stringBuilder = new scala.collection.mutable.StringBuilder()

    stringBuilder.append("\n")
    stringBuilder.append(accuracy)
    stringBuilder.append("\n\n")
    stringBuilder.append(metrics.confusionMatrix.toString(10000, 10000))
    stringBuilder.append("\n\n")
    DataType.values.foreach(t => stringBuilder.append(t.toString.padTo(20, " ").mkString + "-->\t" + metrics.precision(t.id.toDouble) + "\n"))
    stringBuilder.append("\n")

    println(stringBuilder)
  }

  def train(trainingDataFrame: DataFrame): PipelineModel = {
    val naiveBayes = new NaiveBayes()
      .setSmoothing(1.0)
      .setModelType("multinomial")
      .setLabelCol("label")
      .setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(naiveBayes))

    pipeline.fit(trainingDataFrame)
  }

  def learn(args: String*): Array[String] = {

    val test = sqlContext.createDataFrame(args.map(content => {
      (content, Util.string2Vector(content, 100))
    })).toDF("content", "features")

    model.transform(test)
      .select("content", "probability", "prediction")
      .collect()
      .map {
        case Row(content: String, prob: Vector, prediction: Double) =>
          val predictionDataType = DataType.apply(prediction.toInt)
          val predictionString = s"$content --> $predictionDataType\n\n"
          val probArray = prob.toArray
          val probabilityString = Array.tabulate(probArray.length) { i => DataType(i).toString.padTo(20, ' ') + "-->\t" + probArray(i) + "\n" }
          predictionString ++ probabilityString.mkString
        case _ => "error"
      }
  }
}
