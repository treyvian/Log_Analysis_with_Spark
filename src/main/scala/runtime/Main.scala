package runtime

import org.apache.spark.{SparkConf, SparkContext};
import logic.LogStruct;

object HelloScala {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Log Analysis")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("src/main/scala/resources/log.txt")

    //word count

    val counts = textFile.flatMap(line => line.split('\n'))

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());

    //counts.saveAsTextFile("/tmp/shakespeareWordCount")

    sc.stop()
  }

}
