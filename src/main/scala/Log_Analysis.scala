package src.main.scala

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, SaveMode}
//import org.apache.spark.sql.functions.{col, regexp_extract, udf, to_timestamp, avg, min, max}
import org.apache.spark.sql.functions._

object Log_Analysis {

  def clean_input(df: sql.DataFrame): sql.DataFrame = {

    val reg_host = "^([^\\s]+\\s)"
    val reg_timestamp = "\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\]"
    val reg_path = "[A-Z]+\\s+[\\d\\D]+\\sHTTP\\S+"
    val reg_status = "\"\\s+(\\d{3})"
    val reg_content_size = "\\s+(\\d+)\\s\""

    val regexDF: sql.DataFrame = df.withColumn("host", regexp_extract(col("_c0"), reg_host, 0))
      .withColumn("timestamp", regexp_extract(col("_c0"), reg_timestamp, 0))
      .withColumn("path", regexp_extract(col("_c0"), reg_path, 0))
      .withColumn("status", regexp_extract(col("_c0"), reg_status, 0))
      .withColumn("content_size", regexp_extract(col("_c0"), reg_content_size, 0)).drop("_c0")

    val u_parse_time_udf = udf(parse_clf_time)

    val cleanDF: sql.DataFrame = regexDF
      .withColumn("timestamp", to_timestamp(u_parse_time_udf(regexDF("timestamp")), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("status", regexp_extract(col("status"), "\\d{3}", 0).cast("Integer"))
      .withColumn("content_size", regexp_extract(col("content_size"), "\\d+", 0).cast("Integer"))

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

  def contentSizeStats (cleanDF: sql.DataFrame) {
    println("Printing the statistics over the content sizes of the HTTP Requests")
    val sizeStatsDF = cleanDF.select(avg("content_size").as("Average"),
                                      max("content_size").as("Max"),
                                      min("content_size").as("Min"))
    sizeStatsDF.show()
  }

  def httpStatusStats (cleanDF: sql.DataFrame) {
    println("Printing the HTTP code statistics")
    val statusDF = cleanDF.groupBy("status").count().orderBy(asc("status"))
    statusDF.show()
  }

  def frequentHosts (cleanDF: sql.DataFrame) {
    println("Top 10 frequent hosts sending a request to the server")
    val frequentHostsDF: sql.DataFrame = cleanDF.groupBy("host")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    frequentHostsDF.show()
  }

  def frequentPath (cleanDF: sql.DataFrame) {
    println ("Top 10 path in the log")
    val frequentPathDF: sql.DataFrame = cleanDF.groupBy("path")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    frequentPathDF.show(false)
  }


  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sc: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Log_Analysis")
      .getOrCreate()

    sc.sparkContext.setLogLevel("WARN")

    if (args.length < 1) {
      print("Usage: Log_data <file_dataset>")
      sys.exit(1)
    }
  
    val filePath = args(0)

    val df: sql.DataFrame = sc.read.option("header", value = false)
      .option("delimiter", "\n")
      .csv(filePath)

    val line_count = df.count()
    println("The number of lines in the log file in input is:" + line_count)

    val cleanDF: sql.DataFrame = clean_input(df)
    cleanDF.printSchema()
    cleanDF.show(5, false)

    val cleanCount = cleanDF.count()
    println("Lines after cleaning up:" + cleanCount)

    contentSizeStats(cleanDF)
    httpStatusStats(cleanDF)
    frequentHosts(cleanDF)
    frequentPath(cleanDF)

    // val savePath = "/output/dataoutput" 
    // cleanDF.write.mode(SaveMode.Overwrite).format("csv").save(savePath)
    
    sc.stop()
  }
}
