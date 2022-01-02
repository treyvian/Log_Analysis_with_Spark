sbt clean package

spark-submit --class src.main.scala.Log_Analysis target/scala-*/src-main-scala_2.12-*.*.jar data/log10.txt 