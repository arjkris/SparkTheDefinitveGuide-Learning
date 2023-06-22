import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App_2_chap11_DataSets {

  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[12]").setAppName("DataSets").set("spark.sql.shuffle.partitions","5")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val data = spark.read
      .format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .load("D:/Arjun/Data/Csv")

    import spark.implicits._
    val flightDataSet = data.as[Flight]

    var metaData = spark.range(500).map(x=>(x,scala.util.Random.nextLong)).withColumnRenamed("_1","count")
      .withColumnRenamed("_2","randomData").as[FlightMetaData]

    var weGotType = flightDataSet.first().DEST_COUNTRY_NAME //Option[String]

    var collect = flightDataSet.collect()//collect gives an ArrayBackTo the Driver

    //JOIN
    val dataSet = flightDataSet.joinWith(metaData,flightDataSet.col("count") === metaData.col("count"))
    val dataFrame = flightDataSet.join(metaData,Seq("count"))//JVM Type safe is lost

    flightDataSet.groupBy("DEST_COUNTRY_NAME").count().explain()
    flightDataSet.groupByKey(x=>x.DEST_COUNTRY_NAME).count().explain()

    val a = flightDataSet.groupByKey(x=>x.DEST_COUNTRY_NAME).mapValues(x=>1).count()
    a.foreach(x=>println(x._1+"  "+x._2))
  }
}
