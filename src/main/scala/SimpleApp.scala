package main.scala

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

//
//class PartitionerAwareUnionRDDIterator[T: ClassManifest](iters : Seq[Iterator[T]]) extends Iterator[T]{
//  var remainingSeqs = iters
//
//  var currentIter : Iterator[T]  = Iterator()
//
//  def hasNextRecursive(seqs : Seq[Iterator[T]]) : Boolean = {
//    if(seqs.isEmpty)
//      false
//    else if(seqs.head.hasNext)
//      true
//    else
//      hasNextRecursive(seqs.tail)
//  }
//
//  override def hasNext = {
//    currentIter.hasNext || hasNextRecursive(remainingSeqs)
//  }
//
//  override def next : T = {
//    if(currentIter.hasNext)
//      currentIter.next()
//    else{
//      if(remainingSeqs.isEmpty)
//        throw new NoSuchElementException("next on empty iterator")
//      currentIter = remainingSeqs.head
//      remainingSeqs = remainingSeqs.tail
//      this.next
//    }
//  }
//}


object SimpleApp {
  def main(args: Array[String]) {

    test1(args)


  }


  def unionByCogroup[K : ClassManifest, V : ClassManifest](rdds : Seq[RDD[(K,V)]]) = {
    require(rdds.length >= 2 && rdds.length <=3, "Only support 2 or 3 input RDDs")
    require(rdds.map(_.partitioner).distinct.toSet.size == 1, "All input RDD must have the same partitioner" + rdds.map(_.partitioner))
    require(rdds.map(_.partitions.length).distinct.toSet.size == 1, "All input RDD must have the same number of partitions" + rdds.map(_.partitions.length))

    val cogrouped : RDD[(K, Seq[V])] =
      if(rdds.length == 2){
        rdds.head.cogroup(rdds.tail.head).mapValues(tp => tp._1 ++ tp._2)
      }else{
        rdds.head.cogroup(rdds.tail.head, rdds.tail.tail.head).mapValues(tp => tp._1 ++ tp._2 ++ tp._3)
      }

    val result = cogrouped.mapPartitions(iter => iter.flatMap(kvp => kvp._2.map(value => (kvp._1, value))), true)
    result
  }


  def test1(args: Array[String]){
    val sc = new SparkContext(args(0), "Simple App", "../incubator-spark/",  Seq("./target/scala-2.9.3/sql-streaming_2.9.3-1.0.jar"))

    sc.setCheckpointDir("/home/peter/sqlStreaming/tmp/", true)

    var base = sc.makeRDD(1 until 10000).map(i => (i,i%2)).partitionBy(new HashPartitioner(5)).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)

    var counter = 0



    while(true){
      counter += 1
      val startTime = System.currentTimeMillis()
      val add = sc.makeRDD(1 until 10000).map(i => (i,0)).partitionBy(new HashPartitioner(5))

      println("base before filter:" + base + " #p:"+ base.partitions.length)
      base = base.filter(_._2 != 0)

      println("base:" + base + " #p:"+ base.partitions.length)
      println("add:" + add + " #p:" + add.partitions.length)

      //base = unionByCogroup(Seq(base, add))
      base = new PartitionerAwareUnionRDD[(Int, Int)](sc, Seq(base, add))

      println("base after union:" + base + " #p:"+ base.partitions.length)


      base.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      if(counter % 10 == 0)
        base.checkpoint()



      println(base.count())

      println("base after checkpoint:" + base + " #p:"+ base.partitions.length)
      //println("checkpointed:" + base.getCheckpointFile)
      println("Exec Time:" + (System.currentTimeMillis() - startTime))

    }
  }

  def test2(args: Array[String]){
    val sc = new SparkContext(args(0), "Simple App", "../incubator-spark/",  Seq("./target/scala-2.9.3/sql-streaming_2.9.3-1.0.jar"))

    sc.setCheckpointDir("/home/peter/sqlStreaming/tmp/", true)

    var base = sc.makeRDD(1 until 10000, 5).map(i => (i,i%2))

    var counter = 0



    while(true){
      counter += 1
      val startTime = System.currentTimeMillis()
      val add = sc.makeRDD(1 until 10000, 5).map(i => (i,0))


      println("base before filter:" + base + " #p:"+ base.partitions.length)
      base = base.filter(_._2 != 0)

      println("base:" + base + " #p:"+ base.partitions.length)
      println("add:" + add + " #p:" + add.partitions.length)

      base = base.union(add).coalesce(5, false)

      println("base after union:" + base + " #p:"+ base.partitions.length)


      base.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      if(counter % 10 ==0)
        base.checkpoint()



      println(base.count())

      println("base after checkpoint:" + base + " #p:"+ base.partitions.length)
      println("checkpointed:" + base.getCheckpointFile)
      println("Exec Time:" + (System.currentTimeMillis() - startTime))

    }
  }

}



