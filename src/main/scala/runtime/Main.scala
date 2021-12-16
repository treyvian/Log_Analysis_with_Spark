package runtime

//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract, substring, substring_index, trim}


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sc: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Log_Analysis")
      .getOrCreate()
    //val conf = new SparkConf().setAppName("Log Analysis").setMaster("local")
    //val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val log_lines = sc.textFile("src/main/scala/resources/log.txt")
    val df: sql.DataFrame = sc.read.option("header", value = false)
                    .option("delimiter", "\n")
                    .csv("src/main/scala/resources/log10.txt")

    val regexDf: sql.DataFrame = df.withColumn("host",regexp_extract(col("_c0"),
                        "^([^\\s]+\\s)",0))
                      .withColumn("timestamp",regexp_extract(col("_c0"),
                        "\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\]", 0))
                      .withColumn("path",regexp_extract(col("_c0"),
                        "[A-Z]+\\s+[\\d\\D]+\\sHTTP\\S+",0))
                      .withColumn("status",regexp_extract(col("_c0"),
                        "\"\\s+(\\d{3})",0))
                      .withColumn("content_size",regexp_extract(col("_c0"),
                        "\\s+(\\d+)\\s\"",0)).drop("_c0")

    sc.stop()
  }



  def parse_clf_time(s: String): String={
    """ Convert Common Log time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """

    val month_map: Map[String, Int] = Map("Jan"->1, "Feb"->2, "Mar"->3,
                        "Apr"->4, "May"->5, "Jun"->6,
                        "Jul"->7, "Aug"->8, "Sep"->9,
                        "Oct"->10, "Nov"->11, "Dec"->12)

    return "%0:04d-%1:02d-%2:02d %3:02d:%4:02d:%5:02d".format(
      s.substring(7, 11).toInt,
      month_map(s.substring(3, 6)),
      s.substring(0, 2).toInt,
      s.substring(12, 14).toInt,
      s.substring(15, 17).toInt,
      s.substring(18, 20).toInt )
  }
}
