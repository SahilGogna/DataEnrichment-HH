package constants

trait HadoopConstants {

  // configuration constants
  val CONFIG_PATH_1: String = "/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"
  val CONFIG_PATH_2: String = "/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"

  // general constants
  val HADOOP_ROOT_FOLDER_PATH: String = "/user/fall2019/sahilgogna/stm"
  val PATH_SEPARATOR: String = "/"
  val DATA_DIRECTORY_PATH: String = "/Users/sahilgogna/Desktop/stm"
  val FILE_EXTENSION: String = ".txt"
  val FILE_LIST: List[String] = List("calendar_dates", "trips", "frequencies")
  val COPYING_MESSAGE :String = "Copying files to HDFS.."
  val COPIED_MESSAGE : String = "Copied files to HDFS."



}
