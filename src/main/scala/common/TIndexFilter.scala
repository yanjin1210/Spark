package common

import dao.CDXReader
import org.apache.spark.sql.DataFrame
import util.EnvUtil

trait TIndexFilter {
  val cdxReader = new CDXReader()
  val (spark, sc) = EnvUtil.take()
  import spark.implicits._

  def filterData(dataDF: DataFrame, datapath: String): DataFrame

  def getFiltered(datapath: String): DataFrame = {
    val dataDF = cdxReader.getCDXTable(datapath)
    try {
      filterData(dataDF, datapath)
    }
    catch {
      case ex: Throwable => {
        println(ex.getMessage)
        println(ex.getCause)
        null
      }
    }
  }

}
