case class Flight(DEST_COUNTRY_NAME: Option[String],
                  ORIGIN_COUNTRY_NAME: Option[String],
                 count: Int){

}

case class FlightMetaData(
                           count: BigInt,
                           randomData: BigInt
                         ){

}

case class Activity(
                     Arrival_Time: Long,
                     Creation_Time: Long,
                     Device: String,
                     Index: Long,
                     Model: String,
                     User: String,
                     gt: String,
                     x: Double,
                     y: Double,
                     z: Double
                   ){}


case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
case class UserState(user:String,
                     var activity:String,
                     var start:java.sql.Timestamp,
                     var end:java.sql.Timestamp)