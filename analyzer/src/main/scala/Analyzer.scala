package org.logstreaming.analyzer

import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object Analyzer {
  def main(args: Array[String]): Unit = {
    // suppress all the Spark logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val config = ConfigFactory.load("config")

    val sparkConf = new SparkConf().setAppName("LogAnalyzer").setMaster(config.getString("sparkCluster"))
    val ssc = new StreamingContext(sparkConf, Seconds(config.getInt("window")))

    // suppress all the Spark logs
    ssc.sparkContext.setLogLevel("ERROR")

    /*
    * https://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    * It's not completely necessary, but let's be safe.
    * */
    ssc.checkpoint("checkpoint")

    /*
    * https://spark.apache.org/docs/2.1.0/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$
    * */
    val lines = KafkaUtils.createStream(
      ssc,
      config.getString("zookeeper"),
      config.getString("group"),
      Map(config.getString("topic") -> config.getInt("numThreads"))
    ).map(_._2)

    val hits = ParseRecords.transform(lines)
    val popularPages = PopularPages.count(hits, config)
    popularPages.foreachRDD(SaveResults.saveToKafka(_, config))

    ssc.start()
    ssc.awaitTermination()

  }
}
