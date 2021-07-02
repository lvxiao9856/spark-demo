package spark.redis.demo.kafka_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.redis.demo.spark_test.OperatorsDemo.{BasicOperators, WindowOperators}
import utils.ForeachWriteToRedis

/**
 * structed streaming source from kafka test class
 */
class StructedStreamingKafkaDemo{

  //-------------------sink----------------------------------------------
  /**
   * write key-value data from a DataFrame to a specific kafka topic specified in an option
   * @param df 读取的kafka streaming data
   * @return ds query
   */
  private def outputToKafkaTopic(df:DataFrame) = {
    val query = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "./checkpoint/kafka")
      .option("kafka.bootstrap.servers","10.154.3.118:9093")
      .option("topic","sun1")
      .start()
    query
  }

  /**
   * write key-value data from a DataFrame to kafka using a topic specified in the data
   * @param df key value topic partition offset timestamp timestampType headers
   * @return
   */
  private def outputTopicToKafka(df:DataFrame) = {
    val query = df
      .selectExpr("topic","CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","host:port")  //sink kafka
      .start()
    query
  }

  /**
   * sink to memory
   * @param df
   * @return
   */
  private def outputToMemory(df:DataFrame)={
    val query = df
      .writeStream
      .queryName("memtable")  //this query name will be the table name
      .outputMode("complete")
      .format("memory")
      .start()
    query
  }

  /**
   * sink to parquet
   * @param df
   * @return
   */
  private def outputToParquet(df:DataFrame)={
    val query = df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", s"./checkpoint/parquet")
      .option("path", "./sink/parquet")
      .start()
    query
  }

  /**
   * foreachBatch sink
   * provides only at-least-once write guarantees.
   * however,you can use the batchId provided to the function as way to duplicate the output and get an exactly-once guarantee.
   * do not work with the continuous processing mode as it fundamentally relies on the micro-batch execution of streaming query.
   * @param df
   * @return
   */
  private def outputWithForeachBatch(df:DataFrame): StreamingQuery ={
    val query = df
      .writeStream
        .outputMode("append")
      .foreachBatch{(batchDF:DataFrame, batchId:Long)=>
        batchDF.persist()
        batchDF.write.format("console").save()
        batchDF.write.format("parquet").save(s"./sink/parquet_${batchId}")
        batchDF.unpersist()
        ()
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query
  }

  private def outputWithForeach(df:DataFrame)={
    val query = df.writeStream
      .outputMode("append")
      .foreach(new ForeachWriteToRedis("10.154.3.119", 7295, "Suyan@2021"))
      .start()
    query
  }
  //------------------------process--------------------------------

  /**
   * 根据DF列进行相应的列操作
   * @param df
   * @return
   */
  private def processDfWithColumns(df:DataFrame)={
    val exprs = df.columns.map(col =>{
      val expr = col match{
        //case "key" => s"CAST(${col} AS STRING)"
        case "value" => s"CAST(${col} AS STRING)"
        case _ => col
      }
      expr
    })
    df.selectExpr(exprs:_*)
  }


}

/**
 * 半生对象，调用半生类私有方法进行相应功能测试
 */
object StructedStreamingKafkaDemo {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Structed Streaming Demo")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.154.3.118:9093") //source kafka
      .option("subscribe", "sun")
      .option("group.id", "ID2")
      .option("includeHeaders", "true")
      .load()
    import spark.implicits._

//    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]


    /**-----------以下是一些process操作-------------------*/
    val testDemo = new StructedStreamingKafkaDemo()
    val df1 = testDemo.processDfWithColumns(df)
    val basicOperators = new BasicOperators(spark)
    val windowOperators = new WindowOperators(spark)

    /**basic operators*/
    //val df2 = basicOperators.basicOperatorDf(df1)
    //val df2 = basicOperators.basicOperatorView(df1)

    /**window operators*/
    //val df2 = windowOperators.windowGroupCount(df1)
    val df2 = windowOperators.winddowWatermarking(df1)


    /**-----------以下是一些sink操作------------------*/
    //val query = testDemo.outputToKafkaTopic(df1)
    //val query = testDemo.outputTopicToKafka(df1)
    //val query = testDemo.outputWithForeachBatch(df1)
    //val query = testDemo.outputToParquet(df1)
    //val query = testDemo.outputWithForeach(df1)

    //print new data to console
//    val query = df2.writeStream
//            .format("console")
//            .start()
    //启动流框架查询,将输出转到控制台
    val query = df2
          .writeStream
          .outputMode("update")  //complete  append(只输出新添加的（原来没有的）Row)  update(只输出那些被修改的Row)
          .format("console")
          //.trigger(Trigger.ProcessingTime("2 seconds"))  //with two-seconds micro-batch interval
          .start()



    query.awaitTermination()
  }
}
