package hive

class HiveClient extends HiveClientConnection {

  def createTables():Unit = {
    val dbName: String = "fall2019_sahilgogna"
    val terminator: String = ","
    val loc : String = "/user/fall2019/sahilgogna/stm"

    val col1: String = s"service_id STRING,\ncalendar_date STRING,\nexception_type INT"
    val tableName1 : String = "ext_calendar_dates"
    val dataLoc1 : String = s"$loc/calendar_dates"
    createTable(dbName,tableName1,col1,terminator,dataLoc1)

    val col2: String = s"trip_id STRING,\nstart_time STRING,\nend_time STRING,\nheadway_secs INT"
    val tableName2 : String = "ext_frequencies"
    val dataLoc2 : String = s"$loc/frequencies"
    createTable(dbName,tableName2,col2,terminator,dataLoc2)

    val col3: String = s"route_id INT,\nservice_id STRING,\ntrip_id STRING,\ntrip_headsign STRING,\ndirection_id STRING,\nshape_id INT,\nwheelchair_accessible INT,\nnote_fr STRING,\nnote_en STRING"
    val tableName3 : String = "ext_trips"
    val dataLoc3 : String = s"$loc/trips"
    createTable(dbName,tableName3,col3,terminator,dataLoc3)

    stmt.close()
    connection.close()
  }

  def createTable(databaseName: String,tableName:String,tableColumns: String,terminator:String ,fileLocation:String):Unit = {
    val generatedQuery: String = s"CREATE EXTERNAL TABLE ${databaseName}.${tableName} (\n${tableColumns}\n)\n"+
      s"ROW FORMAT DELIMITED\nFIELDS TERMINATED BY '${terminator}'\nSTORED AS TEXTFILE\nLOCATION '${fileLocation}'\n"+
      s"tblproperties (\n" +
      """"skip.header.line.count" = "1",""" +
      "\n" +
      """"serialization.null.format" = "")"""

    print(generatedQuery)

    val dropTableQuery = s"DROP TABLE IF EXISTS $databaseName.$tableName"

    stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
    stmt.execute(dropTableQuery)

    stmt.execute(generatedQuery)
  }



}
