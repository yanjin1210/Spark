package customAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


object MapAccumulator extends App{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("wcAcc")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val sc: SparkContext = spark.sparkContext

  class MapAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

    private var wcMap = new mutable.HashMap[String, Long]()

    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new MapAccumulator()

    override def reset(): Unit = wcMap.clear

    override def add(word: String): Unit = {
      val newCount = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCount)
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
      other.value.foreach {
        case (word, count) => {
          val newCount = this.wcMap.getOrElse(word, 0L) + count
          this.wcMap.update(word, newCount)
        }
      }
    }

    override def value: mutable.HashMap[String, Long] = wcMap
  }


  val accu = new MapAccumulator()
  sc.register(accu, name = "wordCountAcc")
  val textRdd = sc.makeRDD(List("Hello", "Spark", "Hello", "Scala"))
  textRdd.foreach(word => accu.add(word))
  println(accu)
  spark.close()
}
