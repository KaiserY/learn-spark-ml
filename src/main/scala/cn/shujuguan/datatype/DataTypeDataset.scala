package cn.shujuguan.datatype

import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by yueyang on 1/12/16.
  */
object DataTypeDataset {

  def makeDataset(sqlContext: SQLContext): Dataset[DataWithLabel] = {

    import sqlContext.implicits._

    val integerDataset = sqlContext.createDataset(DataTypeSeq.integerSeq())

    val integerDataset_1 = sqlContext.createDataset(DataTypeSeq.integerSeq_1())

    val integerDataset_2 = sqlContext.createDataset(DataTypeSeq.integerSeq_2())

    val doubleDataset = sqlContext.createDataset(DataTypeSeq.doubleSeq())

    val doubleDataset_1 = sqlContext.createDataset(DataTypeSeq.doubleSeq_1())

    val doubleDataset_2 = sqlContext.createDataset(DataTypeSeq.doubleSeq_2())

    val doubleDataset_3 = sqlContext.createDataset(DataTypeSeq.doubleSeq_3())

    val randomUnicodeTextDataset = sqlContext.createDataset(DataTypeSeq.randomUnicodeTextSeq())

    val randomAlphanumericTextDataset = sqlContext.createDataset(DataTypeSeq.randomAlphanumericTextSeq())

    val randomAlphabetTextDataset = sqlContext.createDataset(DataTypeSeq.randomAlphabetTextSeq())

    val emailDataset = sqlContext.createDataset(DataTypeSeq.emailSeq())

    val postcodeDataset = sqlContext.createDataset(DataTypeSeq.postcodeSeq())

    val randomFakeEmailDataset = sqlContext.createDataset(DataTypeSeq.randomFakeEmailSeq())

    val randomFakeEmailDataset_1 = sqlContext.createDataset(DataTypeSeq.randomFakeEmailSeq_1())

    val randomFakeEmailDataset_2 = sqlContext.createDataset(DataTypeSeq.randomFakeEmailSeq_2())

    val randomFakeEmailDataset_3 = sqlContext.createDataset(DataTypeSeq.randomFakeEmailSeq_3())

    val randomFakeEmailDataset_4 = sqlContext.createDataset(DataTypeSeq.randomFakeEmailSeq_4())

    val ipv4Dataset = sqlContext.createDataset(DataTypeSeq.ipv4Seq())

    val randomFakeIPv4Dataset = sqlContext.createDataset(DataTypeSeq.randomFakeIPv4Seq())

    val randomFakeIPv4Dataset_1 = sqlContext.createDataset(DataTypeSeq.randomFakeIPv4Seq_1())

    val uuidDataset = sqlContext.createDataset(DataTypeSeq.uuidSeq())

    val dateDataset = sqlContext.createDataset(DataTypeSeq.dateSeq())

    val dateDataset_1 = sqlContext.createDataset(DataTypeSeq.dateSeq_1())

    val timeDataset = sqlContext.createDataset(DataTypeSeq.timeSeq())

    val timeDataset_1 = sqlContext.createDataset(DataTypeSeq.timeSeq_1())

    integerDataset
      .union(integerDataset)
      .union(integerDataset_1)
      .union(integerDataset_2)
      .union(doubleDataset)
      .union(doubleDataset_1)
      .union(doubleDataset_2)
      .union(doubleDataset_3)
      .union(randomUnicodeTextDataset)
      .union(randomAlphanumericTextDataset)
      .union(randomAlphabetTextDataset)
      .union(emailDataset)
      .union(randomFakeEmailDataset)
      .union(randomFakeEmailDataset_1)
      .union(randomFakeEmailDataset_2)
      .union(randomFakeEmailDataset_3)
      .union(randomFakeEmailDataset_4)
      .union(postcodeDataset)
      .union(ipv4Dataset)
      .union(randomFakeIPv4Dataset)
      .union(randomFakeIPv4Dataset_1)
      .union(uuidDataset)
      .union(dateDataset)
      .union(dateDataset_1)
      .union(timeDataset)
      .union(timeDataset_1)
  }
}
