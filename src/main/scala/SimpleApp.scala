package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "../incubator-spark/README.md" // Should be some file on your system
    val sc = new SparkContext(args(0), "Simple App", "../incubator-spark/",  Seq("./target/scala-2.9.3/sql-streaming_2.9.3-1.0.jar"))

    if(!sc.isLocal){
      sc.setCheckpointDir("hdfs://ec2-67-202-49-43.compute-1.amazonaws.com:9000/tmp/", true)
      println("Setting checkpoint hdfs dir")
    }
    else{
      sc.setCheckpointDir("tmp/", true)
      println("Setting checkpoint local dir")
    }

    val logData = sc.textFile(logFile, 2).cache()
    logData.checkpoint()
    println(logData.getCheckpointFile)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}