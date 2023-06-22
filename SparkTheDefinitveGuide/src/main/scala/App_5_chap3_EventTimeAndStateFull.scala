import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQueryListener, Trigger}

object App_5_chap3_EventTimeAndStateFull {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EventTimeAndStateFull").setMaster("local[8]")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.streaming.checkpointLocation", "/tmp")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val Data = spark.read
      .format("json")
      .option("inferSchema", value = true)
      .load("D:/Arjun/Data/Json/JsonLines/activity-data")
    val schema = Data.schema
    Data.printSchema()

    val streamData = spark.readStream
      .format("json")
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .load("D:/Arjun/Data/Json/JsonLines/activity-data/Streaming")

    //Arbitrary StateFull
    def stateHandler(
                      key: String,
                      values: Iterator[Activity],
                      stateLong: GroupState[Long]
                    ): String = {
      val newState = stateLong.getOption.map(_ + values.size).getOrElse(0L)
      stateLong.update(newState)
      key
    }
    import spark.implicits._
    val dataWithState = streamData.as[Activity].groupByKey(x => x.User)
    val abc = dataWithState
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(func = stateHandler)

    val streamingQuery = abc.writeStream.trigger(Trigger.ProcessingTime(60000))
    .foreach(new ForeachWriter[String]{
      override def open(partitionId: Long, epochId: Long): Boolean = {
        true
      }

      override def process(value: String): Unit = {
        ???
      }

      override def close(errorOrNull: Throwable): Unit = {
        ???
      }
    })
      .queryName("Event Time Driven")
      .outputMode("Append")
      .start()

    val activeStreams = spark.streams.active
    activeStreams.foreach(x => println(x))


    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Query Started"+event.id)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("Query in Progress"+event.progress)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query Terminated"+event.id)
      }
    })

    spark.streams.awaitAnyTermination()

  }

}
