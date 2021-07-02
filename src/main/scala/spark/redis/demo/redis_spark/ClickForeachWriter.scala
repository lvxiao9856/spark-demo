package spark.redis.demo.redis_spark

import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis

class ClickForeachWriter(p_host: String, p_port: String) extends ForeachWriter[Row] {

  val host: String = p_host
  val port: String = p_port

  var jedis: Jedis = _

  def connect() = {
    jedis = new Jedis(host, port.toInt)
    jedis.auth("Suyan@2021")
    ()
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    connect()
    return true
  }

  override def process(record: Row) = {
    val asset = record.getString(0)
    val count = record.getLong(1)
    jedis.set(asset, count.toString)
  }

  override def close(errorOrNull: Throwable)={
    jedis.close()

  }

}
