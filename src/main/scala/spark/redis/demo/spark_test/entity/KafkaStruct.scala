package spark.redis.demo.spark_test.entity

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class KafkaStruct(
                        value:String,
                        topic:String,
                        timestamp:Timestamp)
