package cn.shujuguan

import cn.shujuguan.training.{DataTypeTraining, DataTypeRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yueyang on 12/28/15.
  */
object MLDataType {

  val sparkConf = new SparkConf()
    .setAppName("Data Type Test")
    .setMaster("local")
  //      .set("spark.cassandra.connection.host", "crm.chart2.com")
  //      .set("spark.cassandra.auth.username", "cassandra")
  //      .set("spark.cassandra.auth.password", "cassandra")
  //      .set("spark.executor.memory", "4g")
  //    .set("spark.driver.cores", "4")
  //      .set("spark.driver.memory", "4g")
  //      .set("spark.cores.max", "2")

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val mongoConfig = new Configuration()
  mongoConfig.set("mongo.input.uri", "mongodb://test:test@crm.chart2.com:27017/test.sparkdatatype")

  //    val mongoRDD = sparkContext.newAPIHadoopRDD(
  //      mongoConfig,
  //      classOf[MongoInputFormat],
  //      classOf[Object],
  //      classOf[BSONObject])

  val mongoRDD = DataTypeRDD.makeRDD(sparkContext)

  val trainingRDD = mongoRDD.collect().map(n => {
    (n._1, DataTypeTraining.makeFeaturesArray(n._2))
  })

  val dataFrame = sqlContext.createDataFrame(trainingRDD).toDF("label", "features")

  val splits = dataFrame.randomSplit(Array(0.75, 0.25))

  val training = splits(0)
  val test = splits(1)

  val hashingTF = new HashingTF()
    .setInputCol("features")
    .setOutputCol("tf")
  val idf = new IDF()
    .setInputCol("tf")
    .setOutputCol("idfFeatures")
  val naiveBayes = new NaiveBayes()
    .setSmoothing(1.0)
    .setModelType("multinomial")
    .setLabelCol("label")
    .setFeaturesCol("features")
  val pipeline = new Pipeline()
    .setStages(Array(naiveBayes))


  // Fit the pipeline to training documents.
  val model = pipeline.fit(training)

  model.save("/tmp/ml")

  def main(args: Array[String]) {

    //     Prepare test documents, which are unlabeled (id, text) tuples.
    //    val test = sqlContext.createDataFrame(Seq(
    //      makeTuple("2"),
    //      makeTuple("-123"),
    //      makeTuple("123.12"),
    //      makeTuple("1123.345"),
    //      makeTuple("fae@fsef"),
    //      makeTuple("123@132"),
    //      makeTuple("fae@2131.com"),
    //      makeTuple("fae@21@3.1.com"),
    //      makeTuple("fae@21@3.1.com"),
    //      makeTuple("abc"),
    //      makeTuple("sfa32"),
    //      makeTuple("你好"),
    //      makeTuple("今日は"),
    //      makeTuple("150400"),
    //      makeTuple("123456"),
    //      makeTuple("1.2.3.4"),
    //      makeTuple("1.5"),
    //      makeTuple("1.5.2"),
    //      makeTuple("999.999.999.999"),
    //      makeTuple("fae@gbase.cn")
    //    )).toDF("content", "features")
    //
    //    test.select("content", "features")
    //      .collect()
    //      .foreach(n => println(n))

    //    val model = PipelineModel.load("/tmp/ml")

    // Make predictions on test documents.
    val predictions = model.transform(test)
    //    predictions.select("content", "probability", "prediction")
    //      .collect()
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
    println(accuracy)

    val predictionAndLabel = predictions.select("prediction", "label").map(r => (r.getDouble(0), r.getDouble(1)))
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println(metrics.confusionMatrix)
  }

  def learn(args: String*): Array[String] = {
    //    val model = PipelineModel.load("/tmp/ml")
    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = sqlContext.createDataFrame(args.map(content => {
      makeTuple(content)
    })).toDF("content", "features")

    // Make predictions on test documents.
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
        case _ => ""
      }
  }

  def makeTuple(text: String): (String, Vector) = (text, DataTypeTraining.makeFeaturesArray(text))
}
