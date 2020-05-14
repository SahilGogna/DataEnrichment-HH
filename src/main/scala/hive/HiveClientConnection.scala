package hive

import java.sql.{Connection, DriverManager}

trait HiveClientConnection {

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/sahlgogna;user=sahilgogna;password=sahilgogna")
  val stmt = connection.createStatement()
}
