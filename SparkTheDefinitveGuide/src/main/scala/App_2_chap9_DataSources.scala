import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.codehaus.janino.Java

object App_2_chap9_DataSources {
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataSources").setMaster("local[12]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //Turn of the metaData : Success File
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")

    //spark.readStream -> DataStreamReader      // dataFrame.writeStream -> DataStreamWriter
    //spark.read       -> DataFrameReader       // dataFrame             -> DataFrameWriter
    val csvName = "testFile"
    val now = Calendar.getInstance().getTime.getTime

    val test = spark.read
      .option("mode","failFast")   //->Read Modes
      .option("header",true)
      .option("inferSchema",true)
      .format("csv")
      .load("D:/Arjun/Data/Csv")

    val a1 = Array(1,2,3)

    val schema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME",StringType,true),
      new StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      new StructField("count",LongType,true)
    ))

    val test2 = spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .load("D:/Arjun/Data/Csv")
    test2.show(5)


    test.coalesce(1)
      .write.format("csv")
      .option("mode","overWrite")  //->Write Modes
      .option("path",s"D:/Arjun/Data/Output/DataSourcesOutputs/${csvName}/${now}")
      .save()

//    test.write.format("csv")
//      .option("sep","\t")
//      .save(s"D:/Arjun/Data/Output/DataSourcesOutputs/Test.tsv")

    //JSON

    val jsonObj = spark.read.format("json")
      .schema(schema)
      .load("D:/Arjun/Data/Json/JsonObjects")
    jsonObj.show(5)

    val jsonLines = spark.read.format("json")
      .option("multiLine",true)
      .schema(schema)
      .load("D:/Arjun/Data/Json/JsonLines")
    jsonLines.show(5)
  }
}
