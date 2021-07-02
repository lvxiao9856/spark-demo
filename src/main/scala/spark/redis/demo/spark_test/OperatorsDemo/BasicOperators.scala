package spark.redis.demo.spark_test.OperatorsDemo

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed
import spark.redis.demo.spark_test.entity.KafkaStruct

/**
 * structed streaming basic operators:selection,projection,aggregation
 * @param spark sparksession定义
 */
class BasicOperators(spark:SparkSession) {

  import spark.implicits._

  /**
   * DataFrame basic operators
   * @param df
   * @return
   */
  def basicOperatorDf(df:DataFrame): DataFrame ={
    if(!df.isStreaming){
      return df
    }
    //val ds = df.as[KafkaStruct]
    df.select("value").where("topic=\"sun\"")
    df.groupBy("value").count()
  }

  /**
   * DataSet basic operators
   * @param ds
   * @return
   */
  def basicOperatorDs(ds:Dataset[KafkaStruct]): Dataset[(String, Double)] ={
    //implicit val mapenc: Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
    ds.filter(_.topic=="sun").map(_.value)
    ds.groupByKey(_.topic).agg(typed.avg(_.value.toDouble))

  }

  /**
   * temporary view basic operators
   * @param df
   * @return
   */
  def basicOperatorView(df:DataFrame): DataFrame ={
    df.createOrReplaceTempView("kafkaTable")
    spark.sql("select value, count(*) from kafkaTable group by value")
  }
}
