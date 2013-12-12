package main.scala

import java.io._
import java.net.{ServerSocket,Socket,SocketException}
import java.util.Random
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io._
import scala.collection.mutable.ArrayBuffer


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


class FileStreamQueue (sc : SparkContext, file: String, var secPerBatch : Int  ) extends mutable.Queue[RDD[String]]{

  val f = Source.fromFile(file)

  val iter = f.getLines()

  override def dequeue() : RDD[String] = {
    var startTime = 99999999

    var records = ArrayBuffer[String]()

    var time = 0

    while(iter.hasNext && secPerBatch > time -startTime){
      val line = iter.next()
      time = line.split(",")(1).toInt
      if(startTime == 99999999)
        startTime = time
      records += line
    }
    println("Generated " + records.size + " Records")
    sc.makeRDD(records, 5)
  }

  override def size = 1

}




class FileStreamQueueConstRec (sc : SparkContext, file: String, var recPerBatch : Int  ) extends mutable.Queue[RDD[String]]{

  var f = Source.fromFile(file)

  var iter = f.getLines()

  override def dequeue() : RDD[String] = {
    var count = 0
    var records = ArrayBuffer[String]()


    while(count < recPerBatch ){
      if(!iter.hasNext){
        f = Source.fromFile(file)
        iter = f.getLines()
      }
      records += iter.next
      count += 1
    }
    println("TwitterStream: Generated " + records.size + " Records")
    sc.makeRDD(records, 5)
  }

  override def size = 1

}


class FileStreamQueueLBNL (sc : SparkContext, file: String, var recPerBatch : Int  ) extends mutable.Queue[RDD[String]]{

  var f = Source.fromFile(file)

  var iter = f.getLines()

  override def dequeue() : RDD[String] = {
    var count = 0
    var records = ArrayBuffer[String]()


    while(count < recPerBatch ){
      if(!iter.hasNext){
        f = Source.fromFile(file)
        iter = f.getLines()
      }
      val line = iter.next
      val newline = line.split(" ").zipWithIndex.map(tp => {
        val value =
          if(tp._2 == 3 || tp._2 == 5){

            val splitted = tp._1.split("\\.")
            if(splitted.size == 5)
              splitted(0) + "." +splitted(1) + "." +splitted(2) + " " +splitted(3) + "." +splitted(4)
            else
              tp._1
          }
          else{
            tp._1
          }
        value
      }).mkString(" ")

      records += newline
      count += 1
    }
    //println("TwitterStream: Generated " + records.size + " Records")
    sc.makeRDD(records, 5)
  }

  override def size = 1

}