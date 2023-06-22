import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc, expr}
import org.apache.spark.sql.types.{IntegerType, Metadata, StringType, StructField, StructType}

object App_2_chap5_BasicStructured {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Basic Structured")
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val struct = StructType(Array(
      StructField("DEST_COUNTRY_NAME",StringType,nullable = true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,nullable = true),
      StructField("count",IntegerType,nullable = true,metadata = Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("csv")
      //.option("inferSchema",value = true)
      .option("header",value = true)
      .schema(struct)
      .load("D:/Arjun/Data/Csv")
    df.printSchema()
    println(df.columns.mkString(","))

    df.select("count").show(2)
    //df.select("count_1233").show(2) --error : org.apache.spark.sql.AnalysisException: cannot resolve '`count_1233`' given input columns: [DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count]
    //but on runtime

    df.select(expr("(count + 5)*200 - 6")).show(1)
    println(df.where("count>20").count())

    val first = df.first()
    println(first)
    println(first.getString(0)+" : "+first.getString(1)+" : "+first.getInt(2))

    //Creating Rows
    val sampleRow = Row("Hello","Test",1)

    val d = spark.sparkContext.parallelize(Seq(sampleRow))
    val df1 = spark.createDataFrame(d,struct)
    val df3 = df.union(df1)


    println(df.select(col("ORIGIN_COUNTRY_NAME")).show(1))
    println(df3.where("ORIGIN_COUNTRY_NAME = 'Test'").count())
    println(df3.where("ORIGIN_COUNTRY_NAME = 'Test' OR DEST_COUNTRY_NAME = 'United States'").count())

    //Common Ways
    df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME","count").show(1)
    df.select(col("DEST_COUNTRY_NAME"),col("ORIGIN_COUNTRY_NAME")).show(1)
    //df.select(col("DEST_COUNTRY_NAME"),col("ORIGIN_COUNTRY_NAME"),"count").show(1) cannot give both col and string. Compile error

    df.select(expr("DEST_COUNTRY_NAME AS Change")).show(1)
    df.select(expr("DEST_COUNTRY_NAME AS Change").alias("Changed Again")).show(1)

    //Select + expr = SelectExpr
    df.selectExpr("DEST_COUNTRY_NAME AS Change","ORIGIN_COUNTRY_NAME AS O").show(1)
    df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS WithInCountry").show(10)

    //
    df.withColumnRenamed("DEST_COUNTRY_NAME","DEST_COUNTRY_NAME_Changed").show(1)
    df.withColumn("AddingExtra Column",expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show()

    //casting
    //select cast(count as long) from table
    df.select(col("count").cast("Long")).show(3)

    df.where(col("count")>100).where(conditionExpr = ("DEST_COUNTRY_NAME == 'United States'")).show(2)
    df.where(col("count")>100).where(col ("DEST_COUNTRY_NAME") === "United States").show(2)

    println(df.count())
    println(df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").distinct().count())

      //sort and order by
    df.sort(asc("count"),desc("DEST_COUNTRY_NAME")).show(1)
    df.orderBy(expr("count desc"),col("DEST_COUNTRY_NAME")).show(1)

    println(df.rdd.getNumPartitions)
    println(df.repartition(5,col("DEST_COUNTRY_NAME")).rdd.getNumPartitions)

    //df.toLocalIterator()


  }
}
