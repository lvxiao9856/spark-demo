
package spark.redis.demo.kafka_spark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * kafka创建topic：kafka/bin/kafka-topics.sh --create --zookeeper node01:2181 --replication-factor 3 --partitions 3 --topic spark_kafka
 * kafka生产：kafka-console-producer.sh --broker-list node01:9092,node01:9092,node01:9092 --topic spark_kafka
 * kafka查看topic:./kafka-topics.sh --list --zookeeper 10.154.3.118:2181
 * kafka查看topic内容：kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sun --from-beginning
 */

object SparkKafkaTest {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    //spark.master should be set as local[n], n > 1
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(5))//5表示5秒中对数据进行切分形成一个RDD
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("./checkpoint")
    //准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.154.3.118:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkKafkaDemo2",
      //earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      //这里配置latest自动重置偏移量为最新的偏移量,即如果有偏移量从偏移量位置开始消费,没有偏移量从新来的数据开始消费
      "auto.offset.reset" -> "earliest",
      //false表示关闭自动提交.由spark帮你提交到Checkpoint或程序员手动维护
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("sun")
    //2.使用KafkaUtil连接Kafak获取数据
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,//位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))//消费策略,源码强烈推荐使用该策略
    //3.获取VALUE数据
    val lineDStream: DStream[String] = recordDStream.map(_.value())//_指的是ConsumerRecord
    val wrodDStream: DStream[String] = lineDStream.flatMap(_.split(",")) //_指的是发过来的value,即一行数据
    val wordAndOneDStream: DStream[(String, Int)] = wrodDStream.map((_,1)).cache()

    //txt文件流
    val textStream = ssc.textFileStream(".\\src\\resources\\test.txt")

    //使用UpdateStateByKey进行更新
    val result = wordAndOneDStream.updateStateByKey((seq:Seq[Int], option:Option[Int]) => {
      var value = 0
      value += option.getOrElse(0)
      for(elem <- seq){
        value += elem
      }
      Option(value)
    })

    val wordCounts1 = result.reduceByKey(_ + _)

    val wordCounts: DStream[(String, Int)] = wordAndOneDStream.reduceByKey(_+_)
    //wordCounts1.print()
    //println("--------")
    //wordCounts.print()

    // 测试transform
    println("测试transform:")
    val test = new SparkKafkaTest()
    //test.transformTest(textStream)
    //test.noTransformTest(textStream)
    test.transformTest(recordDStream)
    test.noTransformTest(recordDStream)

    ssc.start()//开启
    ssc.awaitTermination()//等待优雅停止
  }
}

class SparkKafkaTest(){
  private def transformTest(data:InputDStream[ConsumerRecord[String, String]]):Unit = {
    println("transform:")
    data.transform(
      rdd => {
        rdd.flatMap(_.value().split(",")).map((_,1)).reduceByKey(_+_)
      }
    ).print()
  }

  private def noTransformTest(data: InputDStream[ConsumerRecord[String, String]]):Unit = {
    println("no transform:")
    data.flatMap(_.value().split(",")).map((_, 1)).reduceByKey(_+_).print()
  }

  private def transformTest[T](data:DStream[T]):Unit={
    data.transform(rdd => {
      rdd.flatMap(_.toString.split(",")).map((_,1)).reduceByKey(_+_)
    })
  }

  private def noTransformTest[T](data: DStream[T]):Unit={
    data.flatMap(_.toString.split(",")).map((_,1)).reduceByKey(_+_).print()
  }
}
