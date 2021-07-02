name := "spark-redis-demo"

version := "0.1"

scalaVersion := "2.12.5"

val sparkVersion = "2.4.0"
val jedisVersion = "2.7.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "redis.clients" % "jedis" % jedisVersion

)
libraryDependencies += "com.redislabs" %% "spark-redis" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.isuper" % "s2-geometry-library-java" % "0.0.1"



