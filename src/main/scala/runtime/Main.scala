package runtime

import org.apache.spark.{SparkConf, SparkContext};
import scala.collection.mutable.ArrayBuffer
import logic.LogStruct;

object HelloScala {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("Log Analysis").setMaster("local")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("src/main/scala/resources/log.txt")

    // Working with rdd
    val logs = textFile.flatMap(line => line.split('\n'))

    logs.foreach(println)

    //System.out.println("Total words: " + logs.count());
    //counts.saveAsTextFile("/tmp/shakespeareWordCount")

    sc.stop()
  }

}
