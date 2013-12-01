package main.scala

import java.io._
import java.net.{ServerSocket,Socket,SocketException}
import java.util.Random
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class TextStreamQueue (sc : SparkContext,
                             var mean:Int,
                             var sd:Int,
                             var recordsPerBatch : Int
                                ) extends mutable.Queue[RDD[String]]{




  override def dequeue() : RDD[String] = {
    val sd = this.sd
    val mean = this.mean
    val recordsPerBatch = this.recordsPerBatch
    sc.makeRDD(1 to recordsPerBatch, 5).map(i => ("" + (scala.util.Random.nextGaussian() * sd + mean).toInt + "," + 1))
  }

  override def size = 1

}