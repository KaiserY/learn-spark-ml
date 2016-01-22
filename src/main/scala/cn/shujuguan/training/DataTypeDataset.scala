package cn.shujuguan.training

/**
  * Created by yueyang on 1/12/16.
  */
object DataTypeDataset {
  //  def makeDataset(sqlContext: SQLContext): Dataset[(Double, String)] = {
  //    val sampleCount = 10000
  //
  //    val integerDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.INTEGER.id.toDouble
  //      val content = scala.util.Random.nextInt().toString
  //      (label, content)
  //    })
  //
  //    val integerDataset_1 = sqlContext.createDataset(Seq.tabulate(sampleCount)(n => {
  //      val label = DataType.INTEGER.id.toDouble
  //      val content = n.toString
  //      (label, content)
  //    }))
  //
  //    val integerDataset_2 = sqlContext.createDataset(Seq.tabulate(sampleCount)(n => {
  //      val label = DataType.INTEGER.id.toDouble
  //      val content = ((n + 1) * -1).toString
  //      (label, content)
  //    }))
  //
  //    val doubleDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.DOUBLE.id.toDouble
  //      val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * 100000000d).toString
  //      (label, content)
  //    })
  //
  //    val doubleDataset_1 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.DOUBLE.id.toDouble
  //      val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * -100000000d).toString
  //      (label, content)
  //    })
  //
  //    val doubleDataset_2 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.DOUBLE.id.toDouble
  //      val content = (scala.util.Random.nextDouble() + scala.util.Random.nextInt()).toString
  //      (label, content)
  //    })
  //
  //    val doubleDataset_3 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.DOUBLE.id.toDouble
  //      val content = ((scala.util.Random.nextDouble() + scala.util.Random.nextInt()) * -1.0).toString
  //      (label, content)
  //    })
  //
  //    val randomUnicodeTextDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.TEXT.id.toDouble
  //      val content = scala.util.Random.nextString(scala.util.Random.nextInt(20) + 1).toString
  //      (label, content)
  //    })
  //
  //    val randomAlphanumericTextDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.TEXT.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(20) + 1).mkString
  //      (label, content)
  //    })
  //
  //    val randomAlphabetTextDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.TEXT.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(20).filter(c => c >= 'A' && c <= 'z').mkString
  //      (label, content)
  //    })
  //
  //    val emailDataset = sqlContext.createDataset(Source.fromInputStream(getClass.getResourceAsStream("/email.csv")).getLines().map(line => {
  //      val label = DataType.EMAIL.id.toDouble
  //      val content = line
  //      (label, content)
  //    }).toSeq)
  //
  //    val postcodeDataset = sqlContext.createDataset(Source.fromInputStream(getClass.getResourceAsStream("/postcode.csv")).getLines().map(line => {
  //      val label = DataType.POST_CODE.id.toDouble
  //      val content = line
  //      (label, content)
  //    }).toSeq)
  //
  //    val randomFakeEmailDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_EMAIL.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1)
  //      (label, content)
  //    })
  //
  //    val randomFakeEmailDataset_1 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_EMAIL.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1) + "." +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
  //      (label, content)
  //    })
  //
  //    val randomFakeEmailDataset_2 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_EMAIL.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
  //      (label, content)
  //    })
  //
  //    val randomFakeEmailDataset_3 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_EMAIL.id.toDouble
  //      val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
  //        scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
  //      (label, content)
  //    })
  //
  //    val randomFakeEmailDataset_4 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_EMAIL.id.toDouble
  //      val content = scala.util.Random.nextInt(1000) + "@" + scala.util.Random.nextInt(1000)
  //      (label, content)
  //    })
  //
  //    val ipv4Dataset = sqlContext.createDataset(Source.fromInputStream(getClass.getResourceAsStream("/ipv4.csv")).getLines().map(line => {
  //      val label = DataType.IP_V4.id.toDouble
  //      val content = line
  //      (label, content)
  //    }).toSeq)
  //
  //    val randomFakeIPv4Dataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_IP_V4.id.toDouble
  //      val content = (scala.util.Random.nextInt(744) + 256) + "." +
  //        (scala.util.Random.nextInt(744) + 256) + "." +
  //        (scala.util.Random.nextInt(744) + 256) + "." +
  //        (scala.util.Random.nextInt(744) + 256)
  //      (label, content)
  //    })
  //
  //    val randomFakeIPv4Dataset_1 = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.FAKE_IP_V4.id.toDouble
  //      val content = scala.util.Random.nextInt(1000) + "." +
  //        scala.util.Random.nextInt(1000) + "." +
  //        scala.util.Random.nextInt(1000)
  //      (label, content)
  //    })
  //
  //    val uuidDataset = sqlContext.createDataset(Source.fromInputStream(getClass.getResourceAsStream("/uuid.csv")).getLines().map(line => {
  //      val label = DataType.UUID.id.toDouble
  //      val content = line
  //      (label, content)
  //    }).toSeq)
  //
  //    val dateDataset = sqlContext.createDataset(Seq.fill(sampleCount) {
  //      val label = DataType.DATE.id.toDouble
  //      val content = new LocalDate(
  //        scala.util.Random.nextInt(9999) + 1,
  //        scala.util.Random.nextInt(12) + 1,
  //        scala.util.Random.nextInt(28) + 1
  //      ).toString("yyyy-MM-dd")
  //      (label, content)
  //    })
  //
  //    integerDataset
  //      .union(integerDataset)
  //      .union(integerDataset_1)
  //      .union(integerDataset_2)
  //      .union(doubleDataset)
  //      .union(doubleDataset_1)
  //      .union(doubleDataset_2)
  //      .union(doubleDataset_3)
  //      .union(randomUnicodeTextDataset)
  //      .union(randomAlphanumericTextDataset)
  //      .union(randomAlphabetTextDataset)
  //      .union(emailDataset)
  //      .union(randomFakeEmailDataset)
  //      .union(randomFakeEmailDataset_1)
  //      .union(randomFakeEmailDataset_2)
  //      .union(randomFakeEmailDataset_3)
  //      .union(randomFakeEmailDataset_4)
  //      .union(postcodeDataset)
  //      .union(ipv4Dataset)
  //      .union(randomFakeIPv4Dataset)
  //      .union(randomFakeIPv4Dataset_1)
  //      .union(uuidDataset)
  //      .union(dateDataset)
  //  }
}
