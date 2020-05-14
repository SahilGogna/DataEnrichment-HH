name := "DataEnrichment-HH"

version := "0.1"

scalaVersion := "2.13.2"

val hadoopVersion = "2.7.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _ % hadoopVersion)

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

// Cloudera artifacts are published in their own remote repository
resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"