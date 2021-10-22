package Application

import Service.FilterJobPosts
import common.TApplication

object TestFilterJobs extends App with TApplication{
  start(){
    val dataPath = "C:/data/cdx-00000.gz"
    val jobFiltered = new FilterJobPosts()
    jobFiltered.getFiltered(dataPath)
  }
}
