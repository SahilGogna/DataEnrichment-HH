package hadoop

import java.io.File

import hive.HiveClient

object Test extends App {
/*
  val dirPath: String = "/Users/sahilgogna/Desktop/stm"
  val sourceUrl : String = "http://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"

  val downloadFiles = new UnzipService()
  downloadFiles.unzipUrl(sourceUrl,dirPath)

  val t:HadoopService = new HadoopService()
  t.putFiles()

 */
  val hc = new HiveClient()
  hc.createTables()

}
