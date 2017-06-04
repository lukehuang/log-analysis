package org.logstreaming.analyzer

import java.util

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

trait Save[T] {
  def process(partition: Iterator[T]): Unit
  def save(results: RDD[T]): Unit = {
    results.foreachPartition(partition => process(partition))
  }}

abstract class SaveResults[T] extends Save[T] with Serializable

class Kafka[T](implicit val config: Config) extends SaveResults[T] {
  def process(partition: Iterator[T]): Unit = {
    val kafkaOpTopic = config.getString("resultsTopic")
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    partition.foreach(
      record => {
        val data = record.toString
        val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
        producer.send(message)
      })

    producer.close()
  }
}
