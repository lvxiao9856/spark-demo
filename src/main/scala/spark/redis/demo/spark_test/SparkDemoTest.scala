package spark.redis.demo.spark_test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, _}

import scala.collection.mutable

object SparkDemoTest {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val logFile = "./src/resources/access.log" // Should be some file on your system
    val sparkConf = new SparkConf()
      .setAppName("appName")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.network.timeout", "360s")

    // RDD SparkContext
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7)))
    rdd1.rightOuterJoin(rdd2).foreach(println(_))
    //groupByKey 返回spark的数据结构CompactBuffer 即kv._2
    rdd1.union(rdd2).groupByKey().map(kv=>(kv._1,kv._2.toList)).foreach(println(_))
    sc.stop()
    //SQL SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    //test pivot
    val pivot_data = spark.read.format("csv").option("header", "true").load("./src/resources/struct_data.csv")
    val column_expr = pivot_data.columns.map(map_fun)
    val pivot_data2 = pivot_data.select(column_expr:_*)
    pivot_data2.printSchema()

    val expr1 = gen_expr(pivot_data.columns)
    pivot_data.printSchema()
    println(expr1)
    println(expr1.head)
    println(expr1.tail)
    val basic_roll =pivot_data.groupBy("userid")
      .pivot("month")
      .agg(expr1.head,expr1.tail:_*)  // _*将1 to 5转化为参数序列
      .na.fill(-1)
    basic_roll.show()


    pivot_data.groupBy("userid")
      .agg(collect_set(concat_ws("_",$"month",$"id1")).alias("location"))
      .map(row=>(row.getAs[mutable.WrappedArray[String]]("location"),row.getAs[String]("userid")))
      .toDF("months","userid").show()

    //DF to RDD groupby

    pivot_data2.groupBy("userid").sum("id1").show()
    val rdd_data = pivot_data.rdd.map(row=> {
      val userid = row.getAs[String]("userid")
      val month = row.getAs[String]("month")
      val id1 = row.getAs[String]("id1")
      TestClass(userid, month, id1.toInt)
    }).groupBy(_.userid).mapValues(_.toList.sortBy(_.id1))
    rdd_data.foreach(println(_))

    pivot_data.rdd.map(row=> {
      val userid = row.getAs[String]("userid")
      val month = row.getAs[String]("month")
      val id1 = row.getAs[String]("id1")
      TestClass(userid, month, id1.toInt)
    }).sortBy(_.month).foreach(print(_))

    pivot_data.rdd.map(row=> {
      val userid = row.getAs[String]("userid")
      val month = row.getAs[String]("month")
      val id1 = row.getAs[String]("id1")
      TestClass(userid, month, id1.toInt)
    }).keyBy(_.userid).foreach(println(_))

    spark.stop()
  }

  def gen_expr(feature_array: Array[String]): Seq[Column] = {
    val expr =feature_array.toSeq.filter(x=>x!="userid" && x!="month")
      .flatMap(x=>Seq(functions.sum(x).alias(x.toString.replace("-","_"))))
    expr
  }

  def map_fun(column:String)={
    if (column.equals("userid") || column.equals("month")){
      col(column).cast("String")
    }else{
      col(column).cast("Int")
    }
  }
}

case class TestClass(userid:String,month:String,id1:Int) extends Serializable