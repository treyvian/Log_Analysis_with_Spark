package main.scala.utils

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}

object Utils {

  /** Parse the dataframe in input, which contains only one column with the log * string and parse it into multiple columns based on the common log format
  *
  * @param df spark dataframe to be cleaned
  * @return cleaned spark dataframe 
  */
    def clean_input(df: sql.DataFrame): sql.DataFrame = {

      val reg_host = "^([^\\s]+\\s)"
      val reg_timestamp = "\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\]"
      val reg_path = "[A-Z]+\\s+[\\d\\D]+\\sHTTP\\S+"
      val reg_status = "\"\\s+(\\d{3})"
      val reg_content_size = "\\s+(\\d+)\\s\""

      val regexDF: sql.DataFrame = df.withColumn("host", fun.regexp_extract(col("_c0"), reg_host, 0))
          .withColumn("timestamp", fun.regexp_extract(col("_c0"), reg_timestamp, 0))
          .withColumn("path", fun.regexp_extract(col("_c0"), reg_path, 0))
          .withColumn("status", fun.regexp_extract(col("_c0"), reg_status, 0))
          .withColumn("content_size", fun.regexp_extract(col("_c0"), reg_content_size, 0)).drop("_c0")

      val u_parse_time_udf = udf(parse_clf_time)

      val cleanDF: sql.DataFrame = regexDF
          .withColumn("timestamp", fun.to_timestamp(u_parse_time_udf(regexDF("timestamp")), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("status", fun.regexp_extract(col("status"), "\\d{3}", 0).cast("Integer"))
          .withColumn("content_size", fun.regexp_extract(col("content_size"), "\\d+", 0).cast("Integer"))

      return cleanDF
    }

  /** Starting from a timestamp not parsed, map it into a new string that can 
  * be correctly. it is meant to be a support function for clean_input
  *
  * @param timestamp string to be parse 
  */
  def parse_clf_time: String => String = (timestamp: String) => {
    
    val month_map: Map[String,Int] = Map(
        "Jan" -> 1, "Feb" -> 2, "Mar" -> 3,
        "Apr" -> 4, "May" -> 5, "Jun" -> 6,
        "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
        "Oct" -> 10, "Nov" -> 11, "Dec" -> 12
      )

    "%04d-%02d-%02d %02d:%02d:%02d" format(
        timestamp.substring(8, 12).toInt,
        month_map(timestamp.substring(4, 7)),
        timestamp.substring(1, 3).toInt,
        timestamp.substring(13, 15).toInt,
        timestamp.substring(16, 18).toInt,
        timestamp.substring(19, 21).toInt
      )
  }
}