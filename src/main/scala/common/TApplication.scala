package common

import org.apache.spark.sql.SparkSession
import util.EnvUtil

trait TApplication {
  def start(master:String = "local[*]", applicationName:String = "Application")(op: => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(applicationName)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    EnvUtil.put(spark, sc)

    try {
      op
    }
    catch {
      case ex: Throwable => println(ex.getMessage + "\n" + ex.getCause)
    }

    spark.close()
    EnvUtil.clear()
  }
}
