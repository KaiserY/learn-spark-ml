package cn.shujuguan

object App {
  def main(args: Array[String]) {
    "11".getBytes("Unicode").foreach(b => println((b & 0xff)))

    val countArray: Array[Int] = Array.fill(5) {
      0
    }

    println(countArray.mkString(" "))
  }
}
