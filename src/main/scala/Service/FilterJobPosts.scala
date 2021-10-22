package Service

import common.TIndexFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable

class FilterJobPosts extends TIndexFilter{
  import spark.implicits._
  override def filterData(dataDF: DataFrame, dataPath: String): DataFrame = {

    val jobTerms = mutable.HashSet("job", "career", "jobs", "careers")
    val splitter = "(/|_|%|-|\\?|=|\\(|\\)|&|\\+|\\.)"
    val isJobUrl = udf((url:String) =>{
      val urlLowered = url.toLowerCase.split(splitter)
      urlLowered.exists(word => jobTerms.contains(word))
    })

    val filteredDF = dataDF.where('languages === "eng" &&
      'status === 200 && 'mime === "text/html" && isJobUrl('url)).select(
      col("url"),
      col("filename"),
      col("offset"),
      col("length"))
    filteredDF.write.mode("overwrite").option("header", value = true).csv("output/jobs/" + dataPath.split("/").takeRight(1)(0))

    filteredDF
  }
}
