import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object App_3_chap3_SharedVariables {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Shared Variables")
      .set("spark.sql.shuffle.partitions","5")
      //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[Flight],classOf[customAccumulator]))

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val data = spark.sparkContext.parallelize("Spark The Definitive Guide : Big Data Processing Made Simple".split(" "))
    val broadcastLookUp = Map("Spark" -> 1000, "Definitive" -> 200,"Big" -> -300, "Simple" -> 100)
    val broadcast = spark.sparkContext.broadcast(broadcastLookUp)

    println(broadcast.value)
    println(broadcast.id)
    //broadcast.destroy()
    println(broadcast.value)

    import spark.implicits._
    val flightData = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .load("D:/Arjun/Data/Csv")
      .as[Flight]

    val unnamed = new LongAccumulator
    spark.sparkContext.register(unnamed)

    spark.sparkContext.longAccumulator("china")
    val named = new LongAccumulator
    spark.sparkContext.register(named,"china")

    flightData.filter(x=>x.DEST_COUNTRY_NAME.isDefined && x.ORIGIN_COUNTRY_NAME.isDefined).foreach(x=>{
      if(x.DEST_COUNTRY_NAME.get == "China" || x.ORIGIN_COUNTRY_NAME.get == "China"){
        named.add(x.count)
      }
    })

    println(named.value)

    val customAccumulator = new customAccumulator
    spark.sparkContext.register(customAccumulator)

    spark.streams.awaitAnyTermination()
  }

  class customAccumulator extends AccumulatorV2[BigInt,BigInt] {
    private var incrementor: BigInt = 0

    override def isZero: Boolean = ???

    override def copy(): AccumulatorV2[BigInt, BigInt] = ???

    override def reset(): Unit = ???

    override def add(v: BigInt): Unit = ???

    override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = ???

    override def value: BigInt = ???
  }
}
