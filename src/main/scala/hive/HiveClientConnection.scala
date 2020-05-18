package hive

import java.sql.{Connection, DriverManager, Statement}

import constants.HiveConstants

trait HiveClientConnection extends HiveConstants{

  Class.forName(DRIVER_NAME)
  val connection: Connection = DriverManager.getConnection(CONNECTION_URL)
  val stmt: Statement = connection.createStatement()
}
