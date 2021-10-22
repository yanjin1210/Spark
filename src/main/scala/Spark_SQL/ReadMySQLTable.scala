package Spark_SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadMySQLTable extends App{
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._

  def getLocalSQLTable(table: String): DataFrame = {
    spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("user", "JinYan")
    .option("password", "1loveMySQL:)")
    .option("dbtable", table)
    .load()
  }

  getLocalSQLTable("demodatabase.customers").show

  spark.close()

}
