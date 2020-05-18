package hive

class HiveClient extends HiveClientConnection {

  def createTables(): Unit = {
    val dbName: String = "fall2019_sahilgogna"
    val terminator: String = ","
    val loc: String = "/user/fall2019/sahilgogna/stm"

    val col1: String = s"service_id STRING,\ncalendar_date STRING,\nexception_type INT"
    val tableName1: String = "ext_calendar_dates"
    val dataLoc1: String = s"$loc/calendar_dates"
    createTable(dbName, tableName1, col1, terminator, dataLoc1)

    val col2: String = s"trip_id STRING,\nstart_time STRING,\nend_time STRING,\nheadway_secs INT"
    val tableName2: String = "ext_frequencies"
    val dataLoc2: String = s"$loc/frequencies"
    createTable(dbName, tableName2, col2, terminator, dataLoc2)

    val col3: String = s"route_id INT,\nservice_id STRING,\ntrip_id STRING,\ntrip_headsign STRING,\ndirection_id STRING,\nshape_id INT,\nwheelchair_accessible INT,\nnote_fr STRING,\nnote_en STRING"
    val tableName3: String = "ext_trips"
    val dataLoc3: String = s"$loc/trips"
    createTable(dbName, tableName3, col3, terminator, dataLoc3)

    enrichTrip(dbName)

    stmt.close()
    connection.close()
  }

  def createTable(databaseName: String, tableName: String, tableColumns: String, terminator: String, fileLocation: String): Unit = {
    val generatedQuery: String = s"CREATE EXTERNAL TABLE ${databaseName}.${tableName} (\n${tableColumns}\n)\n" +
      s"ROW FORMAT DELIMITED\nFIELDS TERMINATED BY '${terminator}'\nSTORED AS TEXTFILE\nLOCATION '${fileLocation}'\n" +
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

  def enrichTrip(databaseName:String): Unit = {

    val createEnrichedTable = "CREATE TABLE fall2019_sahilgogna.enriched_trip(\n" +
      "route_id INT,\nservice_id STRING,\nrip_id STRING,\ntrip_headsign STRING,\n" +
      "direction_id STRING,\nshape_id INT,\nnote_fr STRING,\nnote_en STRING,\n" +
      "start_time STRING,\nend_time STRING,\nheadway_secs INT,\ncalendar_date STRING,\nexception_type INT\n)"+
      "PARTITIONED BY (wheelchair_accessible INT)\n"+
      "stored as PARQUET\n"+
      "tblproperties(\"parquet.compression\"=\"GZIP\")"

    val enrichTrip = "INSERT OVERWRITE TABLE fall2019_sahilgogna.enriched_trip\nPARTITION(wheelchair_accessible)\n"+
    "SELECT t.route_id,\nt.service_id,\nt.trip_id,\nt.trip_headsign,\nt.direction_id,\nt.shape_id,\nt.note_fr,\n"+
      "t.note_en,\nt.wheelchair_accessible,\nf.start_time,\nf.end_time,\nf.headway_secs,\nc.calendar_date,\n"+
      "c.exception_type\nfrom fall2019_sahilgogna.ext_trips t\nINNER JOIN fall2019_sahilgogna.ext_frequencies f"+
      "\nINNER JOIN fall2019_sahilgogna.ext_calendar_dates c\non t.trip_id = f.trip_id and t.service_id = c.service_id"

    stmt.execute(s"DROP TABLE IF EXISTS $databaseName.enriched_trip")
    stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
    stmt.execute(createEnrichedTable)
    stmt.execute(enrichTrip)
  }


}
