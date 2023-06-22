import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object App_3_chap1_Rdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Learning Rdd").set("spark.sql.shuffle.partitions","5")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir("D:/tmp")


    // DF to rdd
    val dfToRdd = spark.range(10).toDF().rdd.map(x=>x.getLong(0))

    import spark.implicits._
    //rdd to df
    val rddToDF = dfToRdd.toDF()

    val myRdd = sc.parallelize(Array(1,2,3,4,5),3)
    myRdd.setName("My List of Numbers")
    println(myRdd.name)
    myRdd.foreach(x=>println(x))

    val textRdd = sc.textFile("D:/Arjun/Data/TextFiles")
    textRdd.foreach(x=>println(x))

    println(textRdd.filter(x=>x.startsWith("t")).count())

    textRdd.map(x=>{(x,x(0),x.startsWith("t"))}).foreach((x)=>println(x._1 + " : "+ x._2 + " : " + x._3))

    println(textRdd.flatMap(x=>x.toSeq).take(5).mkString(","))

    println(textRdd.sortBy(x=>x.length()* -1 ).take(2).mkString(", "))

    val arrayOfRdd = textRdd.randomSplit(Array[Double](0.5,0.5))

    //Actions
    val reduce = sc.range(1,50).reduce((x,y)=>x+y).toString
    println(reduce)

    val longestWordUsingReduce = textRdd.reduce((x,y)=>{
      if(x.length>y.length)
        x
        else
        y
    })
    println(longestWordUsingReduce)

    println(textRdd.count())
    println(textRdd.countByValue())


    //textRdd.saveAsTextFile("D:/Arjun/Data/Output/Result")
    //textRdd.saveAsTextFile("D:/Arjun/Data/Output/ResultCompressed",classOf[BZip2Codec])
    //textRdd.saveAsObjectFile("D:/Arjun/Data/Output/SequenceFile")

    textRdd.checkpoint()

    val fromCheckPoint = textRdd.map(x=>(x,1))
    fromCheckPoint.foreach(println(_))

    println(fromCheckPoint.mapPartitions(x=>Iterator[Int](1)).sum())

    val rs = fromCheckPoint.mapPartitionsWithIndex((partitionIndex,iterator)=>{
      iterator.toList.map(values => s" partitionIndex ${partitionIndex} , values: ${values}").iterator
    }).collect()
    rs.foreach(x=>println(x))

    fromCheckPoint.foreachPartition(iter=>{
      import java.io._
      import scala.util.Random

      val rand = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/randomfile-${rand}.txt"))
      while (iter.hasNext){
        pw.write(iter.next().toString())
      }
      pw.close()
    })

    spark.streams.awaitAnyTermination()
  }
}
