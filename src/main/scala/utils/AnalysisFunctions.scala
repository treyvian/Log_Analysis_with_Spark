package main.scala.utils

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{functions => fun}

/** Contains the methods for the Log analysis */
object AnalysisFunctions {  

    /** some statistics about the sizes of content being returned by the web 
    * server. In particular, the average, minimum, and maximum content sizes
    *
    * @param cleanDF spark dataframe containing the logs
    * @return spark dataframe with content size information
    */
    def contentSizeStats (cleanDF: sql.DataFrame): sql.DataFrame = {
        println("Printing the statistics for the content size of the http body")
        val sizeStatsDF = cleanDF.select(fun.avg("content_size").as("Average"),
                                        fun.max("content_size").as("Max"),
                                        fun.min("content_size").as("Min"))
        return sizeStatsDF
    }

    /** the status values that appear in the data and how many times.
    *
    * @param cleanDF spark dataframe containing the logs
    * @return spark dataframe for each row a status code and the number of times 
    * it appears in the logs
    */
    def httpStatusStats (cleanDF: sql.DataFrame): sql.DataFrame = {
        val statusDF = cleanDF.groupBy("status").count().orderBy(fun.asc("status"))

        return statusDF
    }

    /** sort the number of hosts that did a request on the server ordered by the
    * most frequent requester
    *
    * @param cleanDF spark dataframe containing the logs
    * @param count The minimum frequency to show in the results(). 
    * NOTE: if count is too low the Spark would have a problem saving the file
    * especially if the size in input is quite large 
    *
    * @return spark dataframe with the hosts ordered by the most frequent
    * requester
    */
    def frequentHosts (cleanDF: sql.DataFrame, count: Integer = 5): sql.DataFrame =  {

        if(count < 0){
            throw new RuntimeException("count must be a positive number")
        }

        val frequentHostsDF: sql.DataFrame = cleanDF.groupBy("host")
            .count()
            .filter(col("count") >= count)
            .orderBy(fun.desc("count"))
        
        return frequentHostsDF
    }

    /** Finds the most frequent paths requested on the server ordering them by the
    * number of times they are requested
    *
    * @param cleanDF spark dataframe containing the logs
    * @param isErrorPath boolean value to find the path that weren't accessible
    * at the time of the request
    * @param count The minimum number of times it appears in the log. 
    * NOTE: if count is too low the Spark would have a problem saving the file
    * especially if the size in input is quite large 

    * @return spark dataframe with the most frequent paths
    */
    def frequentPath (cleanDF: sql.DataFrame, isErrorPath: Boolean = false, count: Integer = 100): sql.DataFrame =  {

        var errorPathDf: sql.DataFrame = cleanDF

        if (isErrorPath){
            errorPathDf = errorPathDf.filter(col("status") =!= 200)
        }

        val frequentPathDF: sql.DataFrame = errorPathDf.groupBy("path")
            .count()
            .filter(col("count") >= count)
            .orderBy(fun.desc("count"))

        return frequentPathDF
    }

    /** Finds how many unique hosts are present in the logs
    *
    * @param cleanDF spark dataframe containing the logs
    * @return the number of hosts inside the logs in input
    */
    def uniqueHostsCount (cleanDF: sql.DataFrame): Long ={
        val numberHosts: Long = cleanDF.select(col("host")).distinct().count()
        return numberHosts
    }

    /** Filter all the rows with the status code in input. If no value is 
    * specified the default code is 404
    *
    * @param cleanDF spark dataframe containing the logs
    * @param code status code with which to filter the dataframe
    * @return spark dataframe with the rows with the status code in input
    */
    def statusCodeFilter (cleanDF: sql.DataFrame, code: Integer = 404): sql.DataFrame ={
        val statusDf = cleanDF.filter(col("status") === code)
        return statusDf
    }
}