import constants.GeneralConstants
import hadoop.{HadoopService, UnzipService}
import hive.HiveClient

object AppStarter extends App with GeneralConstants{

  val downloadFiles = new UnzipService()
  downloadFiles.unzipUrl(SOURCE_URL,DIRECTORY_PATH)

  val t:HadoopService = new HadoopService()
  t.putFiles()

  val hc = new HiveClient()
  hc.createTables()

}
