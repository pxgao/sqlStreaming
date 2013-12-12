package main.scala

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Simple App", System.getenv("SPARK_HOME"),  Seq("./target/scala-2.9.3/sql-streaming_2.9.3-1.0.jar"))

    val a = sc.makeRDD(1 until 5000).map(i => (i%10,i)).partitionBy(new HashPartitioner(5))
    val b = sc.makeRDD(1 until 5000).map(i => (i%10,i)).partitionBy(new HashPartitioner(5))

    var sum = 0L
    var count = 0L
    var loopCount = 0L

    val j = a.join(b)//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    while(true){
      val start = System.currentTimeMillis()
      println(j.count)
      val duration = (System.currentTimeMillis() - start)
      if(loopCount > 10){
        sum += duration
        count += 1
      }

      loopCount += 1
      println("Duration:" + duration + " Avg:" + sum.toDouble/count)
    }



  }
}