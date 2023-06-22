import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App_2_chap10_SparkSql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SPARK SQL").set("spark.sql.warehouse.dir","/tmp/spark-warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

//    val a = spark.sql("SELECT 1+1 AS SUM1,'test' AS Field")
//    a.filter(x=>x.get(0).asInstanceOf[Int]>2 || x.get(1).asInstanceOf[String]=="test").show()
//
//    val flightData = spark.read.format("json")
//      .option("inferSchema",true)
//      .load("D:/Arjun/Data/Json/JsonObjects")
//    flightData.createOrReplaceTempView("Flights")
//    flightData.write.saveAsTable("FlightsHive")
//
//    spark.sql("SELECT * FROM Flights").show(10)

    spark.sql(
      """
        |CREATE TABLE flights_csv (
        |DEST_COUNTRY_NAME STRING,
        |ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
        |count LONG)
        |USING csv OPTIONS (header true, path 'D:/Arjun/Data/Csv')
        |""".stripMargin)

    spark.sql("SELECT * FROM flights_csv").show(5)
//    spark.sql(
//      """
//        |CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights_csv
//        |""".stripMargin)
//    spark.sql("INSERT INTO flights_from_select SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights_csv")//1 stage
//    spark.sql("INSERT INTO flights_from_select SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights_csv LIMIT 20")//2 stages
//    spark.sql("select * from flights_from_select").show(2)

    //VIEW
    spark.sql(
      """
        |CREATE VIEW test_view AS
        |   SELECT * FROM flights_csv WHERE DEST_COUNTRY_NAME = 'United States'
        |""".stripMargin)

    spark.sql("SELECT * FROM test_view").show()

    spark.sql("SHOW DATABASES").show()

    //Complex Types
    spark.sql(
      """
        |CREATE VIEW complex AS
        | SELECT (DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME) AS COUNTRY, COUNT FROM flights_csv
        |""".stripMargin)
    spark.sql("SELECT * FROM complex").show()
    var df = spark.sql("SELECT * FROM complex").toDF()
    df.explain()
  }
}
