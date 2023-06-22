import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object App_5_chap2_StructuredStreamingBasics {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("structuredStreamingBasics")
      .set("spark.sql.shuffle.partitions","5")
      .set("spark.sql.streaming.checkpointLocation","/tmp")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val data = spark.read.format("json")
      .option("inferSchema",true)
      .load("D:/Arjun/Data/Json/JsonLines/activity-data")

    val schema = data.schema
//    schema.foreach(x=>{println(x)})
//    data.show(3)

    //STREAMING
    val streamData = spark.readStream.format("json").schema(schema)
      .option("maxFilesPerTrigger",1)
      .load("D:/Arjun/Data/Json/JsonLines/activity-data/Streaming")

    val distinctActivityCount = streamData.groupBy("gt").count()

    val output = distinctActivityCount.writeStream.queryName("DistinctActivityCount")
      .format("console")
      .trigger(Trigger.ProcessingTime(60000))
      .outputMode("complete")
      .start()

//    for (i <- 1 to 100 ){
//      println("New Select")
//      spark.sql("SELECT * FROM DistinctActivityCount").show()
//      Thread.sleep(1000)
//    }
    val activeStreams = spark.streams.active
    activeStreams.foreach(x=>println(x.name))

    output.awaitTermination()

    //Kafka Integration
//    val dataStream = spark.readStream.format("kafka")
//      .option("kafka.bootstrap.servers","localhost:9092")
//      .option("subscribe","test")
//      .load()
//
//    import spark.implicits._
//    val kafkaDataStream = dataStream.selectExpr("CAST(key as string)","CAST(value as string)")
//      .as[(String,String)]
//      .writeStream
//      .format("kafka")
//      .queryName("KafkaTest")
//      .option("kafka.bootstrap.servers","localhost:9092")
//      .option("topic","fromSpark")
//      .start()
//
//    kafkaDataStream.awaitTermination()



  }
}
