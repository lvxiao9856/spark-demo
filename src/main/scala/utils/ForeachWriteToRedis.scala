package utils

import java.sql.Timestamp

import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * redis-cli -h 10.154.3.119 -p 7295 -a Suyan@2021 -c
 * lrange run 0 100
 * @param host
 * @param port
 * @param pwd
 */
class ForeachWriteToRedis(host: String, port: Int, pwd: String) extends ForeachWriter[Row]() {
  var jedis:Jedis = null

  override def open(partitionId:Long, version:Long): Boolean ={
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(20)
    config.setMaxIdle(5)
    config.setMaxWaitMillis(1000)
    config.setMinIdle(2)
    config.setTestOnBorrow(false)
    val jedisPool = new JedisPool(config,host,port)
    jedis = jedisPool.getResource
    jedis.auth(pwd)
    return true
  }

  override def process(record: Row): Unit ={
    //写入数据到redis
    val key = record.getAs[String]("topic")
    val value = record.getAs[String]("value")
    jedis.lpush(key,value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //关闭连接
    jedis.close()
  }

}
