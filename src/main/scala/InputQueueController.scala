package main.scala

import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 11/26/13
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
class InputQueueController(sc : SparkContext) extends Runnable{
  val inputQueues = mutable.Map[Int, mutable.Queue[RDD[String]]]()

  val p = 2000



  var count = 0
  val m1 = 0
  val m2 = 30

  inputQueues += 9999 -> new TextStreamQueue(sc, m1, 10, 100)
  inputQueues += 9998 -> new TextStreamQueue(sc, m1, 10, 100)
  inputQueues += 9997 -> new TextStreamQueue(sc, m2, 10, 1000)
  //inputQueues += 8888 -> new FileStreamQueue(sc,"data/test.dat", 1000)
  inputQueues += 8888 -> new FileStreamQueue(sc,"data/datafile3hours_filtered.dat", 5)
  inputQueues += 7777 -> new FileStreamQueueConstRec(sc,"data/twitter/twitterstream.txt", 5000)
  inputQueues += 10001 -> new FileStreamQueueLBNL(sc,"data/lbnl/port003.txt", 18000)
  inputQueues += 10002 -> new FileStreamQueueLBNL(sc,"data/lbnl/port008.txt", 18000)
  inputQueues += 10003 -> new FileStreamQueueLBNL(sc,"data/lbnl/port019.txt", 18000)


  def run() {


    while(true){
      if(count%(p*2) == 0){
        inputQueues(9999).asInstanceOf[TextStreamQueue].mean = m1
        println("~~~~~~~~~~~~~x.mean = " + m1)
      }
      if(count%(p*2) == p){
        inputQueues(9999).asInstanceOf[TextStreamQueue].mean = m2
        println("~~~~~~~~~~~~~x.mean = " + m2)
      }
      count +=1
      Thread.sleep(1000)
    }

  }
}
