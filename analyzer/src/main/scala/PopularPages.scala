package org.logstreaming.analyzer

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import com.typesafe.config.Config


object PopularPages {
  def count(hits: DStream[LogRecord], config: Config): DStream[(String, Long)] = {
    val popularPages = hits.map(x => (x.url, 1L))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(config.getInt("windowLength")), Seconds(config.getInt("slideInterval")), 2)

    popularPages
  }
}
