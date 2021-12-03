package runtime

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.regexp_extract


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sc = SparkSession.builder()
      .master("local[1]")
      .appName("Log_Analysis")
      .getOrCreate()
    //val conf = new SparkConf().setAppName("Log Analysis").setMaster("local")
    //val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val log_lines = sc.textFile("src/main/scala/resources/log.txt")
    val df = sc.read.option("header", "false").csv("src/main/scala/resources/log10.txt")

    val resultDF = df.withColumn("host",regexp_extract(col("_c0"),
                        "^([^\\s]+\\s)",0))
                      .withColumn("timestamp",regexp_extract(col("_c0"),
                        "\\[\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\]", 0))
                      .withColumn("path",regexp_extract(col("_c0"),
                        "\"\\w+\\s+([^\\s]+)\\s+HTTP\\S*",0))
                      .withColumn("path",regexp_extract(col("_c0"),
                        "\"\\w+\\s+([^\\s]+)\\s+HTTP\\S*",0))
                      .withColumn("status",regexp_extract(col("_c0"),
                        "\"\\s+(\\d{3})",0))
                      .withColumn("content_size",regexp_extract(col("_c0"),
                        "\\s+(\\d+)\\s\"",0)).drop("_c0")


    resultDF.show()
    //resultDF.foreach(println)
    //resultDF.foreach(println)

    //System.out.println("Total words: " + logs.count());
    //counts.saveAsTextFile("/tmp/shakespeareWordCount")

    sc.stop()
  }

}
