/**
 * @author ${Davide.Pozzoli}
 */

import main.scala.utils.Utils.clean_input
import main.scala.utils.AnalysisFunctions._
import java.{time => time} 

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}

/** Contains the main function */
object LogAnalysis {

  def main(args: Array[String]) {

    // For local excecution
    //Create a SparkContext to initialize Spark
    // val spark: SparkSession = SparkSession.builder()
    //   .master("local")
    //   .appName("Log_Analysis")
    //   .getOrCreate()

    //Create a SparkContext to initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .appName("Log_Analysis")
      .getOrCreate()


    // Checks if the input path has been passed in input
    if (args.length != 2) {
      print("Number of arguments insufficient")
      sys.exit(1)
    }
  
    val filePath = args(0)
    val outputPath = args(1)

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

    // Generating timestamp
    val fmt = time.format.DateTimeFormatter.ofPattern("uu_MM_dd_HH_mm_ss")
    val timezone = "Europe/Rome"
    val timestamp = time.LocalDateTime.now(time.ZoneId.of(timezone)).format(fmt)

    // Funcitons
    contentSizeStats(cleanDF).write
                             .format("com.databricks.spark.csv")
                             .option("header", "true")
                            .save(outputPath + "/analysis"+ timestamp +"/contentSizeStats")
    httpStatusStats(cleanDF).write
                            .format("com.databricks.spark.csv")
                            .option("header", "true")
                            .save(outputPath + "/analysis"+ timestamp +"/httpStatusStats")
    frequentHosts(cleanDF).write
                          .format("com.databricks.spark.csv")
                          .option("header", "true")
                          .save(outputPath + "/analysis"+ timestamp +"/frequentHosts")
    frequentPath(cleanDF).write
                          .format("com.databricks.spark.csv")
                          .option("header", "true")
                          .save(outputPath + "/analysis"+ timestamp +"/frequentPath")
    frequentPath(cleanDF, true).show()
    println("The number of uniques number of hosts is: " + uniqueHostsCount(cleanDF))

    // Stop the current session
    spark.stop()
  }
}
