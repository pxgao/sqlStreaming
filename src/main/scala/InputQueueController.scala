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
  val inputQueues = mutable.Map[Int, TextStreamQueue]()

  val p = 20



  var count = 0
  val m1 = 0
  val m2 = 30

  inputQueues += 9999 -> new TextStreamQueue(sc, m1, 5, 10000)
  inputQueues += 9998 -> new TextStreamQueue(sc, m1, 10, 1000)
  inputQueues += 9997 -> new TextStreamQueue(sc, m2, 10, 1000)


  def run() {


    while(true){
      if(count%(p*2) == 0){
        inputQueues(9999).mean = m1
        println("~~~~~~~~~~~~~x.mean = " + m1)
      }
      if(count%(p*2) == p){
        inputQueues(9999).mean = m2
        println("~~~~~~~~~~~~~x.mean = " + m2)
      }
      count +=1
      Thread.sleep(1000)
    }

  }
}
