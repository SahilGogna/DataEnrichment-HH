package hadoop

import java.io.File

import constants.HadoopConstants
import org.apache.hadoop.fs.Path

class HadoopService extends HadoopConfiguration with HadoopConstants{

  def putFiles(): Unit = {
    println(COPYING_MESSAGE)
    assureExistance(HADOOP_ROOT_FOLDER_PATH)
    FILE_LIST.foreach(folder => createSubFolder(folder))
    val dir : File = new File(DATA_DIRECTORY_PATH)
    val fileArray: Array[File] = dir.listFiles()
    FILE_LIST.foreach( fileName => copyToHdfs(fileName, fileArray))
    println(COPIED_MESSAGE)
  }

  def createSubFolder(folderName: String):Unit = {
    val folderPath = HADOOP_ROOT_FOLDER_PATH + PATH_SEPARATOR + folderName
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
    val fileName = name + FILE_EXTENSION
    val destPath = HADOOP_ROOT_FOLDER_PATH + PATH_SEPARATOR + name
    val fileFound: Array[File] = filesArray.filter( file => file.getName.equals(fileName))
    if(fileFound.length > 0){
      val sourcePath: String = fileFound(0).getAbsolutePath
      fileSystem.copyFromLocalFile(new Path(sourcePath), new Path(destPath))
    }
  }
}
