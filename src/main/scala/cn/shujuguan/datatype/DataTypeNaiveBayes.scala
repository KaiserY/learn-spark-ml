package cn.shujuguan.datatype

import cn.shujuguan.common.{DataType, Util}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yueyang on 1/22/16.
  */
object DataTypeNaiveBayes {
  def main(args: Array[String]) {
    val path = "/tmp/ml"

    val sparkConf = new SparkConf()
      .setAppName("Data Type Training")
      .setMaster("local")

    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkContext)

//    val trainingDataset = DataTypeDataset.makeDataset(sqlContext)
//
//    val dataFrame = trainingDataset.toDF().map(r => {
//      (r.getDouble(0), r.getString(1), Util.string2Vector(r.getString(1), 100))
//    }).toDF("label", "content", "features")

    val trainingRDD = DataTypeRDD.makeRDD(sparkContext)

    val dataFrame = sqlContext.createDataFrame(trainingRDD.map(d => {
      (d.label, d.content, Util.string2Vector(d.content, 100))
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
}
