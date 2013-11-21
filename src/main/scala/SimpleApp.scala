package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/peter/incubator-spark/README.md" // Should be some file on your system
    val sc = new SparkContext(args(0), "Simple App", "/home/peter/incubator-spark/",
        Seq("/home/peter/sqlStreaming/target/scala-2.9.3/sql-streaming_2.9.3-1.0.jar")
        )
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}