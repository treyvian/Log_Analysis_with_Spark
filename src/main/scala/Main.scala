import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract, udf}

object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sc: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Log_Analysis")
      .getOrCreate()

    val filePath = "src/main/scala/resources/log10.txt"

    val df: sql.DataFrame = sc.read.option("header", value = false)
      .option("delimiter", "\n")
      .csv(filePath)

    val line_count = df.count()
    println("The number of lines in the log file in input is:" + line_count)

    val cleanDF: sql.DataFrame = clean_input(df)
    cleanDF.show(5)

    val clean_count = cleanDF.count()
    println("Lines after cleaning up:" + clean_count)
    sc.stop()
  }

  def clean_input(df: sql.DataFrame): sql.DataFrame = {
    val regexDf: sql.DataFrame = df.withColumn("host", regexp_extract(col("_c0"),
      "^([^\\s]+\\s)", 0))
      .withColumn("timestamp", regexp_extract(col("_c0"),
        "\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\]", 0))
      .withColumn("path", regexp_extract(col("_c0"),
        "[A-Z]+\\s+[\\d\\D]+\\sHTTP\\S+", 0))
      .withColumn("status", regexp_extract(col("_c0"),
        "\"\\s+(\\d{3})", 0))
      .withColumn("content_size", regexp_extract(col("_c0"),
        "\\s+(\\d+)\\s\"", 0)).drop("_c0")

    val u_parse_time_udf = udf(parse_clf_time)

    // LocalDateTime
    val cleanDf: sql.DataFrame = regexDf.withColumn("timestamp", u_parse_time_udf(regexDf("timestamp"))
      .as("LocalDateTime"))
      .withColumn("status", regexp_extract(col("status"),
        "\\d{3}", 0))
      .withColumn("content_size", regexp_extract(col("content_size"),
        "\\d+", 0))
    return cleanDf
  }

  val month_map: Map[String, Int] = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3,
    "Apr" -> 4, "May" -> 5, "Jun" -> 6,
    "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
    "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)


  def parse_clf_time: String => String = (s: String) => {
      "%04d-%02d-%02d %02d:%02d:%02d" format(
      s.substring(8, 12).toInt,
      month_map(s.substring(4, 7)),
      s.substring(1, 3).toInt,
      s.substring(13, 15).toInt,
      s.substring(16, 18).toInt,
      s.substring(19, 21).toInt)
  }
}
