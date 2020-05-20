package constants

trait HiveConstants extends HadoopConstants{

  // connection constants
  val DRIVER_NAME : String = "org.apache.hive.jdbc.HiveDriver"
  val CONNECTION_URL : String = "jdbc:hive2://quickstart.cloudera:10000/sahlgogna;user=sahilgogna;password=sahilgogna"

  // database constants
  val DB_NAME : String = "fall2019_sahilgogna"
  val TERMINATOR: String = ","

  // table 1 constants
  val TABLE_1 : String = "ext_calendar_dates"
  val TABLE_1_COLUMNS: String = s"service_id STRING,\ncalendar_date STRING,\nexception_type INT"
  val FOLDER_NAME_1 : String = "calendar_dates"
  val DATA_LOCATION_1: String = s"$HADOOP_ROOT_FOLDER_PATH$PATH_SEPARATOR$FOLDER_NAME_1"

  // table 2 constants
  val TABLE_2: String = "ext_frequencies"
  val TABLE_2_COLUMNS: String = s"trip_id STRING,\nstart_time STRING,\nend_time STRING,\nheadway_secs INT"
  val FOLDER_NAME_2 : String = "frequencies"
  val DATA_LOCATION_2: String = s"$HADOOP_ROOT_FOLDER_PATH$PATH_SEPARATOR$FOLDER_NAME_2"

  // table 3 constants
  val TABLE_3: String = "ext_trips"
  val TABLE_3_COLUMNS: String = s"route_id INT,\nservice_id STRING,\ntrip_id STRING,\ntrip_headsign STRING,\ndirection_id STRING,\nshape_id INT,\nwheelchair_accessible INT,\nnote_fr STRING,\nnote_en STRING"
  val FOLDER_NAME_3 : String = "trips"
  val DATA_LOCATION_3: String = s"$HADOOP_ROOT_FOLDER_PATH$PATH_SEPARATOR$FOLDER_NAME_3"


  // general constants
  val CREATING_TABLES : String = "Creating Tables.."
  val ENRICHING_TRIP : String = "Enriching Trip.."

  // queries
  val STRICT_MODE_QUERY : String = "SET hive.exec.dynamic.partition.mode=nonstrict"
  val ENRICH_TRIP_QUERY : String = "INSERT OVERWRITE TABLE fall2019_sahilgogna.enriched_trip\nPARTITION(wheelchair_accessible)\n"+
    "SELECT t.route_id,\nt.service_id,\nt.trip_id,\nt.trip_headsign,\nt.direction_id,\nt.shape_id,\nt.note_fr,\n"+
    "t.note_en,\nf.start_time,\nf.end_time,\nf.headway_secs,\nc.calendar_date,\n"+
    "c.exception_type,\nt.wheelchair_accessible\nfrom fall2019_sahilgogna.ext_trips t\nLEFT JOIN fall2019_sahilgogna.ext_frequencies f"+
    "\non t.trip_id = f.trip_id\nLEFT JOIN fall2019_sahilgogna.ext_calendar_dates c on t.service_id = c.service_id"


  val CREATE_ENRICHED_TABLE_QUERY : String = "CREATE TABLE fall2019_sahilgogna.enriched_trip(\n" +
    "route_id INT,\nservice_id STRING,\nrip_id STRING,\ntrip_headsign STRING,\n" +
    "direction_id STRING,\nshape_id INT,\nnote_fr STRING,\nnote_en STRING,\n" +
    "start_time STRING,\nend_time STRING,\nheadway_secs INT,\ncalendar_date STRING,\nexception_type INT\n)"+
    "PARTITIONED BY (wheelchair_accessible INT)\n"+
    "stored as PARQUET\n"+
    "tblproperties(\"parquet.compression\"=\"GZIP\")"

}
