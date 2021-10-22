package dao

import org.apache.spark.sql.DataFrame
import util.EnvUtil

class HiveTables {
  private val (spark, sc) = EnvUtil.take()
  import spark.implicits._

  def showDataBases(): Unit = spark.sql("show databases").show

  def useDataBases(db: String): Unit = {
    spark.sql(s"create database if not exists $db")
    spark.sql(s"use $db")
  }

  def showTables(): Unit = {
    spark.sql("show tables").show
  }

  def readTable(table: String): DataFrame = {
    spark.sql(s"select * from $table")
  }

  def writeTable(df: DataFrame, table: String): Unit = {
    df.writeTo(table).createOrReplace()
  }
}
