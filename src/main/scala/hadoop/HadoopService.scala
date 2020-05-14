package hadoop

import java.io.File

import org.apache.hadoop.fs.Path

class HadoopService extends HadoopConfiguration {

  val fileList: List[String] = List("calendar_dates", "trips", "frequencies")
  val rootFolderPath : String = "/user/fall2019/sahilgogna/stm"
  val pathSeparator = "/"
  val dataDirPath: String = "/Users/sahilgogna/Desktop/stm"
  val fileExtension: String = ".txt"

  def putFiles(): Unit = {
    assureExistance(rootFolderPath)
    fileList.map(folder => createSubFolder(folder))
    val dir : File = new File(dataDirPath)
    val fileArray: Array[File] = dir.listFiles()
    fileList.map( fileName => copyToHdfs(fileName, fileArray))

  }

  def createSubFolder(folderName: String):Unit = {
    val folderPath = rootFolderPath + pathSeparator + folderName
    assureExistance(folderPath)
  }

  def assureExistance(folderPath:String):Boolean = {
    val path : Path = new Path(folderPath)
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
    fileSystem.mkdirs(path)
  }

  def copyToHdfs(name:String , filesArray: Array[File]):Unit = {
    val fileName = name + fileExtension
    val destPath = rootFolderPath + pathSeparator + name
    val fileFound: Array[File] = filesArray.filter( file => file.getName().equals(fileName))
    if(fileFound.length > 0){
      val sourcePath: String = fileFound(0).getAbsolutePath()
      fileSystem.copyFromLocalFile(new Path(sourcePath), new Path(destPath))
    }
  }
}
