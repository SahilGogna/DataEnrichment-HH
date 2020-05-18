package hadoop

import java.io.{File, FileOutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

import constants.GeneralConstants

class UnzipService extends GeneralConstants{

  def unzipUrl(sourceUrl:String, dirPath : String) : Unit = {
    println(DOWNLOADING_MESSAGE)
    val url = new URL(sourceUrl)
    val connection = url.openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(METHOD_TYPE_GET)
    val in = connection.getInputStream
    val zipIn = new ZipInputStream(in)
    val buffer = new Array[Byte](1024)
    var zipEntry: ZipEntry = zipIn.getNextEntry
    val directory = new File(dirPath)
    if (!directory.exists()) {
      directory.mkdir()
    } else if (directory.length() > 0) {
      directory.delete()
      new File(dirPath)
    }

    while (zipEntry != null) {
      val newFile: File = new File(dirPath, zipEntry.getName)
      val os: FileOutputStream = new FileOutputStream(newFile)
      var len: Integer = zipIn.read(buffer)
      while (len > 0) {
        os.write(buffer, 0, len)
        len = zipIn.read(buffer)
      }
      os.close()
      zipEntry = zipIn.getNextEntry

    }
    println(DOWNLOADED_MESSAGE)
    zipIn.closeEntry()
    zipIn.close()
  }

}
