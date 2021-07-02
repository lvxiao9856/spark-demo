package spark.redis.demo

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}

object SparkStreamingDemo {

  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit =  {
//    if (args.length < 1) {
//      System.err.println("Usage: StreamingWordCount <directory>")
//      System.exit(1)
//    }

    //创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    // Create the context
    //创建StreamingContext对象，与集群进行交互
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./checkpoint")

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //如果目录中有新创建的文件，则读取
    val path = "E:\\程序\\spark-redis-demo\\src\\resources"

    //val lines = ssc.textFileStream(path)

    val lines = ssc.socketTextStream("10.154.3.118", 9999)
    //分割为单词
    val words = lines.flatMap(_.split(" "))
    //统计单词出现次数
    val word = words.map((_,1)).cache()

    //使用UpdateStateByKey进行更新
    val result = word.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      var value = 0
      value += option.getOrElse(0)
      for(elem <- seq){
        value += elem
      }
      Option(value)
    })

    val wordCounts = word.reduceByKey(_ + _)
    val wordCounts1 = result.reduceByKey(_ + _)

    //打印结果
    wordCounts.print()
    println("--------")
    wordCounts1.print()
    //启动Spark Streaming
    ssc.start()
    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }
}
