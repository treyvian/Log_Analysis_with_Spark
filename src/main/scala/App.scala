/**
 * @author ${Davide.Pozzoli}
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import main.scala.utils.Utils.clean_input
import main.scala.utils.AnalysisFunctions._
import java.{time => time} 
import java.io._

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.sql.{functions => fun}

/** Contains the main function */
object LogAnalysis {

  def main(args: Array[String]) {

    // Checks if the input path has been passed in input
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "Exactly 3 arguments are required: <Master> <inputPath> <outputPath>")
    }

    val master = args(0)
    if(master != "local" && master != "yarn"){
      println(master)
      throw new IllegalArgumentException("Master must be local or yarn")
    }

    //Create a SparkContext to initialize Spark
    val spark = SparkSession.builder()
          .master(master)
          .appName("Log_Analysis")
          .getOrCreate()

    
    spark.sparkContext.setLogLevel("WARN")

    println("Starting execution timer")
    val t1 = System.nanoTime

    val filePath = args(1)
    val outputPath = args(2)

    // Read log file 
    val df: sql.DataFrame = spark.read.option("header", value = false)
      .option("delimiter", "\n")
      .csv(filePath)


    val line_count = df.count()
    println("The number of lines in the log file in input is:" + line_count)

    // Preprocessing
    val cleanDF: sql.DataFrame = clean_input(df)

    val cleanCount = cleanDF.count()
    println("Lines after cleaning up:" + cleanCount)

    // Funcitons
    val contentSize = contentSizeStats(cleanDF)
    val stats = contentSize.withColumn("unique_host", lit(uniqueHostsCount(cleanDF))) 

    val httpStatusStats_df = httpStatusStats(cleanDF)
    val frequentHosts_df = frequentHosts(cleanDF)
    val frequentPath_df = frequentPath(cleanDF)

    val duration = (System.nanoTime - t1) / 1e9d
    println("Time taken for the execution:" + duration)
    println("Writing to file")

    // Generating timestamp
    val fmt = time.format.DateTimeFormatter.ofPattern("uu_MM_dd_HH_mm_ss")
    val timezone = "Europe/Rome"
    val timestamp = time.LocalDateTime.now(time.ZoneId.of(timezone)).format(fmt)

    stats.coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputPath + "/analysis"+ timestamp +"/stats")


    httpStatusStats_df.coalesce(1)
                      .write
                      .format("com.databricks.spark.csv")
                      .option("header", "true")
                      .save(outputPath + "/analysis"+ timestamp +"/httpStatusStats")
    frequentHosts_df.coalesce(1)
                    .write
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .save(outputPath + "/analysis"+ timestamp +"/frequentHosts")
    frequentPath_df.coalesce(1)
                    .write
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("delimiter", "^")
                    .save(outputPath + "/analysis"+ timestamp +"/frequentPath")

    // Stop the current session
    spark.stop()
  }
}
