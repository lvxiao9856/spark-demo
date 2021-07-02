package spark.redis.demo.redis_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * https://github.com/RedisLabs/spark-redis/blob/master/doc/getting-started.md
 */
object SparkRedisTest {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1")

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder()
    .appName("myApp")
    .master("local[*]")
    .config("spark.redis.host", "10.154.3.119")
    .config("spark.redis.port", "7295")
    .config("spark.redis.auth", "Suyan@2021")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    import com.redislabs.provider.redis._
    //Create RDD
    val keysRDD = sc.fromRedisKeyPattern("htkey", 5)
    println(keysRDD.collect())
  }



  //val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
/*  df.write
    .format("org.apache.spark.sql.redis")
    .option("table", "foo")
    .save()

  import com.redislabs.provider.redis.streaming._

  val ssc = new StreamingContext(sc, Seconds(1))
  val redisStream = ssc.createRedisStream(Array("foo", "bar"),
    storageLevel = StorageLevel.MEMORY_AND_DISK_2)*/
}
