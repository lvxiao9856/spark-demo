# kafka
### kafka创建topic：
kafka/bin/kafka-topics.sh --create --zookeeper node01:2181 --replication-factor 3 --partitions 3 --topic spark_kafka
### kafka生产：
kafka-console-producer.sh --broker-list localhost:9092 --topic sun
### kafka查看topic:
./kafka-topics.sh --list --zookeeper 10.154.3.118:2181
### kafka查看topic内容：
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sun --from-beginning

# redis
### 启动集群客户端
redis-cli -h 10.154.3.119 -p 7295 -a Suyan@2021 -c
###
