package cn.shujuguan.training

import cn.shujuguan.{DataType, Features}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yueyang on 1/6/16.
  */
object DataTypeTraining {
  def main(args: Array[String]) {
    val path = "/tmp/ml"

    val sparkConf = new SparkConf()
      .setAppName("Data Type Training")
      .setMaster("local")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val trainingRDD = DataTypeRDD.makeRDD(sparkContext)

    val dataFrame = sqlContext.createDataFrame(trainingRDD.map(n => {
      (n._1, n._2, makeFeaturesArray(n._2))
    })).toDF("label", "content", "features")

    //    val splitsDataFrame = dataFrame.randomSplit(Array(0.75, 0.25))
    //
    //    val trainingDataFrame = splitsDataFrame(0)
    //    val testDataFrame = splitsDataFrame(1)

    train(sparkContext, dataFrame, path)
    test(sparkContext, dataFrame, path)
  }

  def train(sparkContext: SparkContext, trainingDataFrame: DataFrame, path: String): Unit = {
    val naiveBayes = new NaiveBayes()
      .setSmoothing(1.0)
      .setModelType("multinomial")
      .setLabelCol("label")
      .setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(naiveBayes))

    val model = pipeline.fit(trainingDataFrame)

    model.write.overwrite().save(path)
  }

  def test(sparkContext: SparkContext, testDataFrame: DataFrame, path: String): Unit = {
    val model = PipelineModel.load(path)

    val predictions = model.transform(testDataFrame)
    //    predictions.select("content", "probability", "prediction")
    //      .collect()
    //      .take(100)
    //      .foreach {
    //        case Row(content: String, prob: Vector, prediction: Double) =>
    //          val predictionDataType = DataType.apply(prediction.toInt)
    //          println(s"$content --> prob=$prob, prediction=$predictionDataType")
    //      }

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
    stringBuilder.append("\n")
    stringBuilder.append(metrics.confusionMatrix.toString(10000, 10000))
    stringBuilder.append("\n")
    DataType.values.foreach(t => stringBuilder.append(t.toString.padTo(20, " ").mkString + "-->\t" + metrics.precision(t.id.toDouble) + "\n"))
    stringBuilder.append("\n")

    println(stringBuilder)
  }

  def makeFeaturesArray(text: String): Vector = {

    val limit = if (text.length > 100) 100 else text.length

    val unicodePostionFeatureCount = 256 * 2 * 100

    val unicodeNumberFeatureCount = Features.maxId * 101

    var featureIndex, alphabet, number, dot, minus, at, colon = 0

    var featuresMap = new mutable.HashMap[Int, Double]

    val featureText = text.substring(0, limit)

    featureText.getBytes("Unicode").drop(2).foreach(byte => {
      val value = byte & 0xFF

      val index = featureIndex

      featureIndex += 256


      featuresMap += index + value -> 5.0
    })

    featureText.foreach({
      case char if '0' until '9' contains char => number += 1
      case char if 'a' until 'z' contains char => alphabet += 1
      case char if 'A' until 'Z' contains char => alphabet += 1
      case '.' => dot += 1
      case '-' => minus += 1
      case '@' => at += 1
      case ':' => colon += 1
      case _ =>
    })

    featureIndex = unicodePostionFeatureCount

    featuresMap += featureIndex + Features.NUMBER.id * 101 + number -> 10.0
    featuresMap += featureIndex + Features.ALPHABET.id * 101 + alphabet -> 10.0
    featuresMap += featureIndex + Features.DOT.id * 101 + dot -> 20.0
    featuresMap += featureIndex + Features.MINUS.id * 101 + minus -> 20.0
    featuresMap += featureIndex + Features.AT.id * 101 + at -> 20.0
    featuresMap += featureIndex + Features.COLON.id * 101 + colon -> 20.0

    Vectors.sparse(unicodePostionFeatureCount + unicodeNumberFeatureCount, featuresMap.toSeq)
  }
}
