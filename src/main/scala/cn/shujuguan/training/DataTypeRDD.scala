package cn.shujuguan.training

import cn.shujuguan.DataType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{LocalDate, LocalTime}

import scala.io.Source

/**
  * Created by kaisery on 2016/1/3.
  */
object DataTypeRDD {
  def makeRDD(sparkContext: SparkContext): RDD[(Double, String)] = {
    val sampleCount = 10000

    val integerRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.INTEGER.id.toDouble
      val content = scala.util.Random.nextInt().toString
      (label, content)
    })

    val integerRDD_1 = sparkContext.parallelize(Seq.tabulate(sampleCount)(n => {
      val label = DataType.INTEGER.id.toDouble
      val content = n.toString
      (label, content)
    }))

    val integerRDD_2 = sparkContext.parallelize(Seq.tabulate(sampleCount)(n => {
      val label = DataType.INTEGER.id.toDouble
      val content = ((n + 1) * -1).toString
      (label, content)
    }))

    val doubleRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DOUBLE.id.toDouble
      val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * 100000000d).toString
      (label, content)
    })

    val doubleRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DOUBLE.id.toDouble
      val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * -100000000d).toString
      (label, content)
    })

    val doubleRDD_2 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DOUBLE.id.toDouble
      val content = (scala.util.Random.nextDouble() + scala.util.Random.nextInt()).toString
      (label, content)
    })

    val doubleRDD_3 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DOUBLE.id.toDouble
      val content = ((scala.util.Random.nextDouble() + scala.util.Random.nextInt()) * -1.0).toString
      (label, content)
    })

    val randomUnicodeTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.TEXT.id.toDouble
      val content = scala.util.Random.nextString(scala.util.Random.nextInt(20) + 1).toString
      (label, content)
    })

    val randomAlphanumericTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.TEXT.id.toDouble
      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(20) + 1).mkString
      (label, content)
    })

    val randomAlphabetTextRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.TEXT.id.toDouble
      val content = scala.util.Random.alphanumeric.take(20).filter(c => c >= 'A' && c <= 'z').mkString
      (label, content)
    })

    val emailRDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/email.csv")).getLines().map(line => {
      val label = DataType.EMAIL.id.toDouble
      val content = line
      (label, content)
    }).toSeq)

    val postcodeRDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/postcode.csv")).getLines().map(line => {
      val label = DataType.POST_CODE.id.toDouble
      val content = line
      (label, content)
    }).toSeq)

    val randomFakeEmailRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_EMAIL.id.toDouble
      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1)
      (label, content)
    })

    val randomFakeEmailRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_EMAIL.id.toDouble
      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1) + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
      (label, content)
    })

    val randomFakeEmailRDD_2 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_EMAIL.id.toDouble
      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
      (label, content)
    })

    val randomFakeEmailRDD_3 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_EMAIL.id.toDouble
      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
      (label, content)
    })

    val randomFakeEmailRDD_4 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_EMAIL.id.toDouble
      val content = scala.util.Random.nextInt(1000) + "@" + scala.util.Random.nextInt(1000)
      (label, content)
    })

    val ipv4RDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/ipv4.csv")).getLines().map(line => {
      val label = DataType.IP_V4.id.toDouble
      val content = line
      (label, content)
    }).toSeq)

    val randomFakeIPv4RDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_IP_V4.id.toDouble
      val content = (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256) + "." +
        (scala.util.Random.nextInt(744) + 256)
      (label, content)
    })

    val randomFakeIPv4RDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.FAKE_IP_V4.id.toDouble
      val content = scala.util.Random.nextInt(1000) + "." +
        scala.util.Random.nextInt(1000) + "." +
        scala.util.Random.nextInt(1000)
      (label, content)
    })

    val uuidRDD = sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream("/uuid.csv")).getLines().map(line => {
      val label = DataType.UUID.id.toDouble
      val content = line
      (label, content)
    }).toSeq)

    val dateRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DATE.id.toDouble
      val content = new LocalDate(
        scala.util.Random.nextInt(9999) + 1,
        scala.util.Random.nextInt(12) + 1,
        scala.util.Random.nextInt(28) + 1
      ).toString("yyyy-MM-dd")
      (label, content)
    })

    val dateRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.DATE.id.toDouble
      val content = new LocalDate(
        scala.util.Random.nextInt(9999) + 1,
        scala.util.Random.nextInt(12) + 1,
        scala.util.Random.nextInt(28) + 1
      ).toString("yyyy年MM月dd日")
      (label, content)
    })

    val timeRDD = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.TIME.id.toDouble
      val content = new LocalTime(
        scala.util.Random.nextInt(23) + 1,
        scala.util.Random.nextInt(59) + 1,
        scala.util.Random.nextInt(59) + 1
      ).toString("hh:mm:ss")
      (label, content)
    })

    val timeRDD_1 = sparkContext.parallelize(Seq.fill(sampleCount) {
      val label = DataType.TIME.id.toDouble
      val content = new LocalTime(
        scala.util.Random.nextInt(23) + 1,
        scala.util.Random.nextInt(59) + 1,
        scala.util.Random.nextInt(59) + 1
      ).toString("hh时mm分ss庙秒")
      (label, content)
    })

    sparkContext.emptyRDD[(Double, String)]
      .union(integerRDD)
      .union(integerRDD_1)
      .union(integerRDD_2)
      .union(doubleRDD)
      .union(doubleRDD_1)
      .union(doubleRDD_2)
      .union(doubleRDD_3)
      .union(randomUnicodeTextRDD)
      .union(randomAlphanumericTextRDD)
      .union(randomAlphabetTextRDD)
      .union(emailRDD)
      .union(randomFakeEmailRDD)
      .union(randomFakeEmailRDD_1)
      .union(randomFakeEmailRDD_2)
      .union(randomFakeEmailRDD_3)
      .union(randomFakeEmailRDD_4)
      .union(postcodeRDD)
      .union(ipv4RDD)
      .union(randomFakeIPv4RDD)
      .union(randomFakeIPv4RDD_1)
      .union(uuidRDD)
      .union(dateRDD)
      .union(dateRDD_1)
      .union(timeRDD)
      .union(timeRDD_1)
  }
}
