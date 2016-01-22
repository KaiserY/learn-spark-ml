package cn.shujuguan.datatype

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by yueyang on 1/22/16.
  */
object DataTypeRDD {

  def makeRDD(sparkContext: SparkContext): RDD[DataWithLabel] = {

    val integerRDD = sparkContext.parallelize(DataTypeSeq.integerSeq())

    val integerRDD_1 = sparkContext.parallelize(DataTypeSeq.integerSeq_1())

    val integerRDD_2 = sparkContext.parallelize(DataTypeSeq.integerSeq_2())

    val doubleRDD = sparkContext.parallelize(DataTypeSeq.doubleSeq())

    val doubleRDD_1 = sparkContext.parallelize(DataTypeSeq.doubleSeq_1())

    val doubleRDD_2 = sparkContext.parallelize(DataTypeSeq.doubleSeq_2())

    val doubleRDD_3 = sparkContext.parallelize(DataTypeSeq.doubleSeq_3())

    val randomUnicodeTextRDD = sparkContext.parallelize(DataTypeSeq.randomUnicodeTextSeq())

    val randomAlphanumericTextRDD = sparkContext.parallelize(DataTypeSeq.randomAlphanumericTextSeq())

    val randomAlphabetTextRDD = sparkContext.parallelize(DataTypeSeq.randomAlphabetTextSeq())

    val emailRDD = sparkContext.parallelize(DataTypeSeq.emailSeq())

    val postcodeRDD = sparkContext.parallelize(DataTypeSeq.postcodeSeq())

    val randomFakeEmailRDD = sparkContext.parallelize(DataTypeSeq.randomFakeEmailSeq())

    val randomFakeEmailRDD_1 = sparkContext.parallelize(DataTypeSeq.randomFakeEmailSeq_1())

    val randomFakeEmailRDD_2 = sparkContext.parallelize(DataTypeSeq.randomFakeEmailSeq_2())

    val randomFakeEmailRDD_3 = sparkContext.parallelize(DataTypeSeq.randomFakeEmailSeq_3())

    val randomFakeEmailRDD_4 = sparkContext.parallelize(DataTypeSeq.randomFakeEmailSeq_4())

    val ipv4RDD = sparkContext.parallelize(DataTypeSeq.ipv4Seq())

    val randomFakeIPv4RDD = sparkContext.parallelize(DataTypeSeq.randomFakeIPv4Seq())

    val randomFakeIPv4RDD_1 = sparkContext.parallelize(DataTypeSeq.randomFakeIPv4Seq_1())

    val uuidRDD = sparkContext.parallelize(DataTypeSeq.uuidSeq())

    val dateRDD = sparkContext.parallelize(DataTypeSeq.dateSeq())

    val dateRDD_1 = sparkContext.parallelize(DataTypeSeq.dateSeq_1())

    val timeRDD = sparkContext.parallelize(DataTypeSeq.timeSeq())

    val timeRDD_1 = sparkContext.parallelize(DataTypeSeq.timeSeq_1())

    sparkContext.emptyRDD[DataWithLabel]
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
