
package spark.redis.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import spark.redis.demo.redis_spark.ClickForeachWriter

object ClickAnalysis {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //连接Redis，创建一个带有连接参数的spark会话
    val spark = SparkSession
      .builder()
      .appName("redis-example")
      .master("local[*]")
      .config("spark.redis.host", "10.154.3.119")
      .config("spark.redis.port", "7295")
      .config("spark.redis.auth", "Suyan@2021")
      .getOrCreate()

    //设置scheme结构时，我们用clicks命名流
    val clicks = spark.readStream
      .format("redis")
      .option("stream.keys", "clicks")
      .schema(StructType(Array(
      StructField("asset", StringType),
      StructField("cost", LongType)
    ))).load()

    //创建一个数据帧，其中包含按资产分组的数据
    val byasset = clicks.groupBy("asset").count

    //将结果写入Redis
    val clickWriter:ClickForeachWriter = new ClickForeachWriter("10.154.3.119", "7295")

    //启动流框架查询
    val query = byasset
      .writeStream
      .outputMode("update")
      .foreach(clickWriter)
      .start()

    //启动流框架查询,将输出转到控制台
//    val query = byasset
//      .writeStream
//      .outputMode("complete")  //complete  append(只输出新添加的（原来没有的）Row)  update(只输出那些被修改的Row)
//      .format("console")
//      .trigger(Trigger.ProcessingTime("2 seconds"))  //with two-seconds micro-batch interval
//      .start()
    query.awaitTermination()
  }
}
