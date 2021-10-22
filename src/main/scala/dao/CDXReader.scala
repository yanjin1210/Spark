package dao

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel
import util.EnvUtil

import scala.collection.mutable

class CDXReader {

  private val (spark, sc) = EnvUtil.take()

  import spark.implicits._

  def getCDXTable(dataPath: String): DataFrame = {
    val textRdd = sc.textFile(dataPath)
    val contentRdd = textRdd.map(line => "{" + line.split("[{}]")(1) + "}")
    val contentDS = contentRdd.toDS
    val fieldsDF = spark.read.json(contentDS)
    fieldsDF
  }
}
//  val fieldsRdd = contentRdd.map(iter => {
//    for (pair <- iter) yield pair.trim.split("(\":)||(\" :)")
//  }.toSeq)
//  fieldsRdd.persist(StorageLevels.DISK_ONLY)
//  fieldsRdd.checkpoint()
//  fieldsRdd.takeSample(false, 20).foreach(x => println(x.mkString))
//  val setAcc = new SetAccumulator()
//  sc.register(setAcc, name = "SetAcc")
//
//  fieldsRdd.flatMap(iter => {
//    for (arr <- iter) yield (arr(0).trim, 1)
//  }).reduceByKey(_ + _).foreach(println)

  //println(setAcc.value.mkString(", "))
