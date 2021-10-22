package Application

import common.TApplication
import dao.CDXReader


object TestIndexReader extends App with TApplication{
  start(){
    val dataPath = "C:/data/cdx-00000.gz"
    val cdxReader = new CDXReader()
    val dataDF = cdxReader.getCDXTable(dataPath)
  }
}
