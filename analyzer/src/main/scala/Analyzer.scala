package org.logstreaming.analyzer

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import java.util.regex.Pattern


case class LogRecord(ip: String, time: String, method: String, url: String, httpCode: Integer)


object Analyzer {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("config")

    val sparkConf = new SparkConf().setAppName("LogAnalyzer").setMaster(config.getString("sparkCluster"))
    val ssc = new StreamingContext(sparkConf, Seconds(config.getInt("window")))

    /*
    * https://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    * It's not completely necessary, but let's be safe.
    * */
    ssc.checkpoint("checkpoint")

    val pattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    val p = Pattern.compile(pattern)

    /*
    * https://spark.apache.org/docs/2.1.0/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$
    * */
    val lines = KafkaUtils.createStream(
      ssc,
      config.getString("zookeeper"),
      config.getString("group"),
      Map(config.getString("topic") -> config.getInt("numThreads"))
    ).map(_._2)

    val words = lines.flatMap(entry => {
      val m = p.matcher(entry)
      if (m.find())
        Some(LogRecord(m.group(1), m.group(4), m.group(5), m.group(6), m.group(8).toInt))
      else
        None
    })

    println(words)

    words.print()

    val wordCounts = words.map(x => (x.url, 1L))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(config.getInt("windowLength")), Seconds(config.getInt("slideInterval")), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
