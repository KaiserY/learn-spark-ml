package cn.shujuguan.training

import cn.shujuguan.common.DataType
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}

import scala.io.Source

/**
  * Created by yueyang on 12/28/15.
  */
object DataTypeMongo {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Spark Data Type Mongo")
      .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri", "mongodb://test:test@crm.chart2.com:27017/test.sparkdatatype")
    mongoConfig.set("mongo.output.uri", "mongodb://test:test@crm.chart2.com:27017/test.sparkdatatype")

    val sampleCount = 10000

    val integerRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.nextInt().toString)
      bson.put("type", DataType.INTEGER.toString)
      bson.put("value", DataType.INTEGER.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val integerRDD_1 = sparkContext.parallelize(Seq.tabulate(sampleCount)(n => {
      val bson = new BasicBSONObject()
      bson.put("content", n.toString)
      bson.put("type", DataType.INTEGER.toString)
      bson.put("value", DataType.INTEGER.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }))

    val integerRDD_2 = sparkContext.parallelize(Seq.tabulate(sampleCount)(n => {
      val bson = new BasicBSONObject()
      bson.put("content", ((n + 1) * -1).toString)
      bson.put("type", DataType.INTEGER.toString)
      bson.put("value", DataType.INTEGER.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }))

    val doubleRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * 100000000d).toString)
      bson.put("type", DataType.DOUBLE.toString)
      bson.put("value", DataType.DOUBLE.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val doubleRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * -100000000d).toString)
      bson.put("type", DataType.DOUBLE.toString)
      bson.put("value", DataType.DOUBLE.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomUnicodeTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.nextString(scala.util.Random.nextInt(20) + 1).toString)
      bson.put("type", DataType.TEXT.toString)
      bson.put("value", DataType.TEXT.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomAlphanumericTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(20) + 1).mkString)
      bson.put("type", DataType.TEXT.toString)
      bson.put("value", DataType.TEXT.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomAlphabetTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(20).filter(c => c >= 'A' && c <= 'z').mkString)
      bson.put("type", DataType.TEXT.toString)
      bson.put("value", DataType.TEXT.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val emailRDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/email.csv")).getLines().map(line => {
      val bson = new BasicBSONObject()
      bson.put("content", line)
      bson.put("type", DataType.EMAIL.toString)
      bson.put("value", DataType.EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }).toSeq)

    val emailRDD_1 = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/email_1.csv")).getLines().map(line => {
      val bson = new BasicBSONObject()
      bson.put("content", line)
      bson.put("type", DataType.EMAIL.toString)
      bson.put("value", DataType.EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }).toSeq)

    val postcodeRDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/postcode.csv")).getLines().map(line => {
      val bson = new BasicBSONObject()
      bson.put("content", line)
      bson.put("type", DataType.POST_CODE.toString)
      bson.put("value", DataType.POST_CODE.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }).toSeq)

    val randomFakeEmailRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1))
      bson.put("type", DataType.FAKE_EMAIL.toString)
      bson.put("value", DataType.FAKE_EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomFakeEmailRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1) + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString)
      bson.put("type", DataType.FAKE_EMAIL.toString)
      bson.put("value", DataType.FAKE_EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomFakeEmailRDD_2 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString)
      bson.put("type", DataType.FAKE_EMAIL.toString)
      bson.put("value", DataType.FAKE_EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomFakeEmailRDD_3 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString)
      bson.put("type", DataType.FAKE_EMAIL.toString)
      bson.put("value", DataType.FAKE_EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val randomFakeEmailRDD_4 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", scala.util.Random.nextInt(1000) + "@" + scala.util.Random.nextInt(1000))
      bson.put("type", DataType.FAKE_EMAIL.toString)
      bson.put("value", DataType.FAKE_EMAIL.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val ipv4RDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/ipv4.csv")).getLines().map(line => {
      val bson = new BasicBSONObject()
      bson.put("content", line)
      bson.put("type", DataType.IP_V4.toString)
      bson.put("value", DataType.IP_V4.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    }).toSeq)

    val randomFakeIPv4RDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val bson = new BasicBSONObject()
      bson.put("content", (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256))
      bson.put("type", DataType.FAKE_IP_V4.toString)
      bson.put("value", DataType.FAKE_IP_V4.id.toString)
      (java.util.UUID.randomUUID().toString, bson)
    })

    val dataTypeRDD = sparkContext.emptyRDD[(String, BasicBSONObject)]
      .union(integerRDD)
      .union(integerRDD_1)
      .union(integerRDD_2)
      .union(doubleRDD)
      .union(doubleRDD_1)
      .union(randomUnicodeTextRDD)
      .union(randomAlphanumericTextRDD)
      .union(randomAlphabetTextRDD)
      .union(emailRDD)
      .union(emailRDD_1)
      .union(randomFakeEmailRDD)
      .union(randomFakeEmailRDD_1)
      .union(randomFakeEmailRDD_2)
      .union(randomFakeEmailRDD_3)
      .union(randomFakeEmailRDD_4)
      .union(postcodeRDD)
      .union(ipv4RDD)
      .union(randomFakeIPv4RDD)

    dataTypeRDD.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[String],
      classOf[BSONObject],
      classOf[MongoOutputFormat[String, BSONObject]],
      mongoConfig)

    val trainingRDD = sparkContext.newAPIHadoopRDD(
      mongoConfig,
      classOf[MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])

    println(trainingRDD.count())

    val aaRDD = trainingRDD.collect().map(n => (n._2.get("value"), n._2.get("content").toString.toCharArray.map(c => c: Int)))

    aaRDD.foreach(n => println(n._1 + " " + n._2.mkString(" ")))
  }
}
