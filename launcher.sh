sbt clean package

spark-submit --class src.main.scala.Log_Analysis target/scala-*/src-main-scala_*.*-*.*.jar data/log10.txt 