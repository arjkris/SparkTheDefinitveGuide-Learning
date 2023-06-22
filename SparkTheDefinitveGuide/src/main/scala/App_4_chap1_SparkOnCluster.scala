import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object App_4_chap1_SparkOnCluster {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark On Cluster").setMaster("local[*]")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")
      .set("spark.executor.extraJavaOptions","XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    val range1 = spark.range(1,10000000,2)
    val range2 = spark.range(1,10000000,4)

    val rep1 = range1.repartition(5)
    val rep2 = range2.repartition(6)

    val select = rep1.selectExpr("id * 5 as id")
    val select2 = select.join(rep2,"id")
    val res = select2.selectExpr("sum(id)")

    println(res.count())

    res.explain()

    //res.persist(StorageLevel.MEMORY_ONLY_SER)

    spark.streams.awaitAnyTermination()
  }
}
