package util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object EnvUtil {
  private val env = new ThreadLocal[(SparkSession, SparkContext)]()

  def put(spark: SparkSession, sc: SparkContext): Unit = env.set((spark, sc))

  def take(): (SparkSession, SparkContext) = env.get()

  def clear(): Unit = env.remove()
}
