import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, max}
import java.io.Serializable

object App_1_chap1_Intro extends Serializable {

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Intro").setMaster("local[12]").set("spark.sql.shuffle.partitions","5")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()



    val flightCsv = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .format("csv")
      .load("D:/Arjun/Data/Csv")

    //
    flightCsv.printSchema()
    flightCsv.sort("count").explain()

    //
    //first register it as table
    flightCsv.createOrReplaceTempView("MyFlightTable")
    val flightSelect = flightCsv.groupBy("DEST_COUNTRY_NAME").count()
    val sqlSelect = spark.sql("Select count(*) FROM MyFlightTable GROUP BY DEST_COUNTRY_NAME")

    flightSelect.explain()
    sqlSelect.explain()

    //
    println(flightCsv.select(max("count")).collect().mkString(" "))
    println(spark.sql("SELECT MAX(count) FROM MyFlightTable").collect().mkString(" "))


    val topDestination = spark.sql("SELECT  DEST_COUNTRY_NAME, SUM(count) as descount FROM MyFlightTable GROUP BY DEST_COUNTRY_NAME order by descount desc LIMIT 5 ")
    topDestination.show()

    flightCsv.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","descount")
      .orderBy(desc("descount"))
      .limit(5)
      .show()

    import spark.implicits._
    var flightDataset = flightCsv.as[Flight]
    var count = flightDataset.filter(x=>x.DEST_COUNTRY_NAME.isDefined).filter(x=>x.DEST_COUNTRY_NAME.get == "United States").count()
    println(count)


    spark.streams.awaitAnyTermination()
  }
}
