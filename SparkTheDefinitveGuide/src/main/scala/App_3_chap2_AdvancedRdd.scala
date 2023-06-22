import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import scala.util.Random

object App_3_chap2_AdvancedRdd {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Advanced Rdd").setMaster("local[*]").set("spark.sql.shuffle.partitions","5")
      //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[getPart]))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val myRdd = sc.textFile("D:/Arjun/Data/TextFiles")//.repartition(3)

//    val basicKeyValue = myRdd.map(x=>(x,1))
//    basicKeyValue.reduceByKey(_ + _).foreach(x=>println(s"${x._1} : ${x._2}"))
//
//    val keys = basicKeyValue.keys.collect()
//    val values = basicKeyValue.values.collect()
//
//    val kv2 = myRdd.keyBy(x=>x(0))
//    println(kv2.count())
//    println(kv2.distinct().count())
//
//    val a = basicKeyValue.lookup("This is a sample ")
//    a.foreach(x=>println(s"key is present ${x}"))

    val char = myRdd.flatMap(x=>x.toLowerCase.toSeq)
    val keyValue = char.map(x=>(x,1))
//    val reducer = keyValue.reduceByKey((x,y)=>x+y)
//    reducer.foreach(x=>{
//      println(s"${x._1} : ${x._2}")
//    })

//    var group = keyValue.groupByKey().map(x=>(x._1,x._2.reduce((r1,r2)=>r1+r2)))
//    group.foreach(x=>{println(s"${x._1} : ${x._2}")})

    //https://stackoverflow.com/questions/33937625/who-can-give-a-clear-explanation-for-combinebykey-in-spark
//    val valToCombiner = (value:Int) => List(value)
//    val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
//    val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
//    // now we define these as function variables
//    val outputPartitions = 6
//    val c = keyValue
//      .combineByKey(
//        valToCombiner,
//        mergeValuesFunc,
//        mergeCombinerFunc,
//        outputPartitions)
//      .collect()
//    c.foreach(x=>println(s"${x._1} : ${x._2}"))


    val coGroup1 = char.map(x=>(x,new Random().nextDouble()))
//    val coGroup2= char.map(x=>(x,new Random().nextDouble()))
//    val coGroup3 = char.map(x=>(x,new Random().nextDouble()))
//
//    val coGroup = coGroup1.cogroup(coGroup2,coGroup3,10)

    keyValue.join(coGroup1,10).foreach(x=>println(s"${x._1} : ${x._2._1} : ${x._2._2}"))

    //keyValue.repartitionAndSortWithinPartitions(new getPart())

    val testPartition = keyValue.partitionBy(new getPart).mapPartitionsWithIndex((x,y)=>{
      y.map(z=>println(s"${z} : with index ${x}")).toIterator
    }).collect()
    testPartition.foreach(x=>println(x))
    spark.streams.awaitAnyTermination()
  }

  class getPart extends Partitioner {

    override def numPartitions: Int = 10

    override def getPartition(key: Any): Int = {
      if(key.asInstanceOf[Char].equals('t') || key.asInstanceOf[Char].equals('a'))
        return 0
        else return new java.util.Random().nextInt(9)+1
    }
  }

}
