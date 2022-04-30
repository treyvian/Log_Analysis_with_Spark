package main.scala.utils

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}


object AnalysisFunctions {  
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

  def frequentPath (cleanDF: sql.DataFrame, isErrorPath: Boolean = false): sql.DataFrame =  {

    var errorPathDf: sql.DataFrame = cleanDF

    if (isErrorPath){
      errorPathDf = errorPathDf.filter(col("status") =!= 200)
    }

    val frequentPathDF: sql.DataFrame = errorPathDf.groupBy("path")
      .count()
      .orderBy(fun.desc("count"))

    return frequentPathDF
  }

  def uniqueHostsCount (cleanDF: sql.DataFrame): Long ={
    val numberHosts: Long = cleanDF.select(col("host")).distinct().count()
    return numberHosts
  }

  // 4c, 4e

  def statusCodeFilter (cleanDF: sql.DataFrame, code: Integer = 404): sql.DataFrame ={
      val statusDf = cleanDF.filter(col("status") === code)
      return statusDf
  }

  
  def statusCodeCount (cleanDF: sql.DataFrame, code: Integer = 404): Long ={
      val statusDf = cleanDF.filter(col("status") === code).count()
      return statusDf
  }
}