/**
 * @author ${Davide.Pozzoli}
 */

import main.scala.utils.Utils.clean_input
import main.scala.utils.AnalysisFunctions._

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}

/** Contains the main function */
object LogAnalysis {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Log_Analysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Checks if the input path has been passed in input
    if (args.length < 1) {
      print("Number of arguments insufficient")
      sys.exit(1)
    }
  
    val filePath = args(0)

    // Read log file 
    val df: sql.DataFrame = spark.read.option("header", value = false)
      .option("delimiter", "\n")
      .csv(filePath)

    val line_count = df.count()
    println("The number of lines in the log file in input is:" + line_count)

    // Parse the dataframe
    val cleanDF: sql.DataFrame = clean_input(df)

    val cleanCount = cleanDF.count()
    println("Lines after cleaning up:" + cleanCount)

    // Tries the function 
    contentSizeStats(cleanDF).show()
    httpStatusStats(cleanDF).show()
    frequentHosts(cleanDF).show()
    frequentPath(cleanDF).show()
    frequentPath(cleanDF, true).show()
    println("The number of uniques number of hosts is: " + uniqueHostsCount(cleanDF))
    statusCodeFilter(cleanDF).show()
    println(statusCodeCount(cleanDF))

    // val savePath = "/output/dataoutput" 
    // cleanDF.write.mode(SaveMode.Overwrite).format("csv").save(savePath)
    
    spark.stop()
  }
}
