package main.scala

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}

object Utils {
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


  def parse_clf_time: String => String = (s: String) => {
    
    val month_map: Map[String,Int] = Map(
        "Jan" -> 1, "Feb" -> 2, "Mar" -> 3,
        "Apr" -> 4, "May" -> 5, "Jun" -> 6,
        "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
        "Oct" -> 10, "Nov" -> 11, "Dec" -> 12
    )

    "%04d-%02d-%02d %02d:%02d:%02d" format(
        s.substring(8, 12).toInt,
        month_map(s.substring(4, 7)),
        s.substring(1, 3).toInt,
        s.substring(13, 15).toInt,
        s.substring(16, 18).toInt,
        s.substring(19, 21).toInt)
  }

  def contentSizeStats (cleanDF: sql.DataFrame): sql.DataFrame = {
    println("Printing the statistics over the content sizes of the HTTP Requests")
    val sizeStatsDF = cleanDF.select(fun.avg("content_size").as("Average"),
                                      fun.max("content_size").as("Max"),
                                      fun.min("content_size").as("Min"))
    return sizeStatsDF
  }

  def httpStatusStats (cleanDF: sql.DataFrame): sql.DataFrame = {
    println("Printing the HTTP code statistics")
    val statusDF = cleanDF.groupBy("status").count().orderBy(fun.asc("status"))

    return statusDF
  }

  def frequentHosts (cleanDF: sql.DataFrame): sql.DataFrame =  {
    println("Top frequent hosts sending a request to the server")
    val frequentHostsDF: sql.DataFrame = cleanDF.groupBy("host")
      .count()
      .orderBy(fun.desc("count"))
    
    return frequentHostsDF
  }

  def frequentPath (cleanDF: sql.DataFrame): sql.DataFrame =  {
    println ("Top path in the log")
    val frequentPathDF: sql.DataFrame = cleanDF.groupBy("path")
      .count()
      .orderBy(fun.desc("count"))

    return frequentPathDF
  }
}