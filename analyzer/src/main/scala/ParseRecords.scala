package org.logstreaming.analyzer

import java.util.regex.Pattern

import org.apache.spark.streaming.dstream.DStream

object ParseRecords {
  def transform(lines: DStream[String]): DStream[LogRecord] = {
    val pattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    val p = Pattern.compile(pattern)

    lines.flatMap(entry => {
      val m = p.matcher(entry)
      if (m.find())
        Some(LogRecord(m.group(1), m.group(4), m.group(5), m.group(6), m.group(8).toInt))
      else
        None
    })
  }
}
