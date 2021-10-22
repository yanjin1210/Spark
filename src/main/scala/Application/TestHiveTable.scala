package Application

import common.TApplication
import dao.HiveTables

object TestHiveTable extends App with TApplication{
  start(){
    val tableConnent = new HiveTables()
    tableConnent.showDataBases()
    tableConnent.useDataBases("test")
    tableConnent.showDataBases()
    tableConnent.showTables()
  }
}
