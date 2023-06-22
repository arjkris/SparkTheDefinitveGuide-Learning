import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering.KMeans

object App_1_chap2_IntroToToolset {
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[8]").setAppName("introToToolset").set("spark.sql.shuffle.partitions","5")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val retailData = sparkSession.read
      .format("csv").
      option("inferSchema",true)
      .option("header",true)
      .load("D:/Arjun/Data/Csv/retail")

    val schema = retailData.schema

    //Selecting multiple cols using df.selectExpr
    retailData.selectExpr("CustomerID", "(UnitPrice * Quantity) as TotalCost", "InvoiceDate")
      //.where("CustomerID == 17809.0")
      .groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day"))
      .sum("TotalCost")
      .show(5)

    //Selecting multiple cols using df.select
    //retailData.select("CustomerID","InvoiceDate","UnitPrice").show(5)



//    val retailDataStreamCall = sparkSession.readStream
//      .format("csv")
//      .option("maxFilesPerTrigger",1)
//      .schema(schema)
//      .option("header",true)
//      .load("D:/Arjun/Data/Csv/retail/streamIt")
//
//
//    val streamData = retailDataStreamCall.selectExpr("CustomerID", "(UnitPrice * Quantity) as TotalCost", "InvoiceDate")
//      //.where("CustomerID == 17809.0")
//      .groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day"))
//      .sum("TotalCost")
//
//    //streamData.writeStream.format("memory").queryName("retailStream").outputMode("complete").start()
//    streamData.writeStream.format("console").queryName("retailStream").outputMode("complete").start()

    //once tha data is written to memory table, it can be queried in a production sink using below query
//    sparkSession.sql(
//      s"""
//         |Select * from retailStream
//         |""".stripMargin).show(5)



    //ML : Using KMeans Algorithm
//    val prepData = retailData.na.fill(0).withColumn("day_of_week",date_format(col("InvoiceDate"),"EEE")).coalesce(5)
//    val trainData = prepData.where("InvoiceDate < '2011-07-01'")
//    val testData = prepData.where("InvoiceDate >= '2011-07-01'")
//
//    println(testData.count())
//    println(trainData.count())
//
//    val indexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
//    val encoder = new OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")
//
//    val vectorAssembler = new VectorAssembler()
//      .setInputCols(Array("UnitPrice","Quantity","day_of_week_encoded"))
//      .setOutputCol("features")
//
//    val transformationPipeline = new Pipeline()
//      .setStages(Array(indexer,encoder,vectorAssembler))
//
//    val fittedPipeline = transformationPipeline.fit(trainData)
//    val transformedTraining = fittedPipeline.transform(trainData)
//
//    transformedTraining.cache()
//
//    val kMeans = new KMeans()
//      .setK(20)
//      .setSeed(1L)
//
//    val kModel = kMeans.fit(transformedTraining)
//    val transformedTest = fittedPipeline.transform(testData)

    sparkSession.streams.awaitAnyTermination()
  }
}
