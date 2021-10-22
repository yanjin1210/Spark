package crawlData

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadCrawl extends App{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("wcAcc")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val sc: SparkContext = spark.sparkContext
  val dataPath = "C:/data/CC-MAIN-20210723143921-20210723173921-00000.warc.wat.gz"
//  val dataDF = spark.read.textFile(dataPath)
//
//  val sizeOfLine = udf((line:String) => line.size)
  val dataRdd = sc.textFile(dataPath)
  var res = ""
  var offset = 10000
  var len = 10000

  dataRdd.foreach {
    line => {
      if (offset > 0) offset -= line.length
      else if (len > 0) {
        len -= line.length
        res += line + "\n"
      }
    }
  }
  println(res)
}
