package hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopConfiguration {

  // making a confugration object
  val config = new Configuration()

  // adding configuration files of the cluster to the config object
  config.addResource(new Path("/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  config.addResource(new Path("/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val fileSystem = FileSystem.get(config)

}
