package hive

import constants.HiveConstants

class HiveClient extends HiveClientConnection with HiveConstants{

  def createTables(): Unit = {
    println(CREATING_TABLES)

    createTable(DB_NAME, TABLE_1, TABLE_1_COLUMNS, TERMINATOR, DATA_LOCATION_1)
    createTable(DB_NAME, TABLE_2, TABLE_2_COLUMNS, TERMINATOR, DATA_LOCATION_2)
    createTable(DB_NAME, TABLE_3, TABLE_3_COLUMNS, TERMINATOR, DATA_LOCATION_3)

    enrichTrip(DB_NAME)

    stmt.close()
    connection.close()
  }

  def createTable(databaseName: String, tableName: String, tableColumns: String, terminator: String, fileLocation: String): Unit = {
    val generatedQuery: String = s"CREATE EXTERNAL TABLE $databaseName.$tableName (\n$tableColumns\n)\n" +
      s"ROW FORMAT DELIMITED\nFIELDS TERMINATED BY '$terminator'\nSTORED AS TEXTFILE\nLOCATION '$fileLocation'\n" +
      s"tblproperties (\n" +
      """"skip.header.line.count" = "1",""" +
      "\n" +
      """"serialization.null.format" = "")"""

    val dropTableQuery = s"DROP TABLE IF EXISTS $databaseName.$tableName"

    stmt.execute(STRICT_MODE_QUERY)
    stmt.execute(dropTableQuery)

    stmt.execute(generatedQuery)
  }

  def enrichTrip(databaseName:String): Unit = {

    println(ENRICHING_TRIP)
    stmt.execute(s"DROP TABLE IF EXISTS $databaseName.enriched_trip")
    stmt.execute(STRICT_MODE_QUERY)
    stmt.execute(CREATE_ENRICHED_TABLE_QUERY)
    stmt.execute(ENRICH_TRIP_QUERY)
  }


}
