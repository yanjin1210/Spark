package Service

import common.TIndexFilter
import org.apache.spark.sql.DataFrame

class FilterTechJob extends TIndexFilter{
  import spark.implicits._
  override def filterData(dataDF: DataFrame, dataPath: String): DataFrame = {
    dataDF
  }
}
