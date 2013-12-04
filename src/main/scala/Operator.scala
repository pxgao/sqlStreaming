package main.scala

import org.apache.spark.streaming._
import org.apache.spark.rdd._
import scala.collection.mutable
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import scala.actors.Actor
import scala.actors.Actor._

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 10/13/13
 * Time: 9:43 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class Operator extends Logging{
  initLogging()

  var sqlContext : SqlSparkStreamingContext = null
  var parentOperators = mutable.ArrayBuffer[Operator]()
  var childOperators = mutable.ArrayBuffer[Operator]()
  var outputSchema : Schema = null

  def getChildOperators = childOperators
  def execute(exec : Execution) : RDD[IndexedSeq[Any]]
  def replaceParent(oldParent : Operator, newParent : Operator)
  override def toString = this.getClass.getSimpleName + "@" + this.hashCode() + " "

  def toStringWithIndent(offset : Int) : String = {
    def genIndent(offset : Int) : StringBuilder = offset match{
      case 0 => new mutable.StringBuilder()
      case offset => genIndent(offset-1).append(" ")
    }
    genIndent(offset).toString() + toString
  }

  def getFamilyTree(generation : Int) : String = {
    val str = new mutable.StringBuilder()

    def addRecord(op : Operator, offset:Int){
      str.append(op.toStringWithIndent(offset) + "\n")
      op.parentOperators.foreach(parent => addRecord(parent, offset + 2))
    }
    addRecord(this,0)
    str.toString()
  }
}

class InnerJoinOperatorSet(parentCtx : SqlSparkStreamingContext) extends Operator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var tailOperator: Operator = null
  def InnerJoinOperatorOrdering = new Ordering[InnerJoinOperator]{
    def compare(a : InnerJoinOperator, b : InnerJoinOperator) = b.selectivity.compare(a.selectivity)
  }
  val innerJoinOperators = mutable.PriorityQueue[InnerJoinOperator]()(InnerJoinOperatorOrdering)

  def addInnerJoinOperator(op : InnerJoinOperator){
    innerJoinOperators += op
    if(innerJoinOperators.size == 1){
      outputSchema = op.outputSchema
      tailOperator = op
    }
  }

  def addParent(op : Operator){
    parentOperators += op
  }

  def optimize(){
    def build(remainingParents : mutable.Set[Operator],
              remainingOperators : mutable.PriorityQueue[InnerJoinOperator]) : mutable.Set[Operator] = {
      if(remainingOperators.size == 0){
        return remainingParents
      }else{
        val op = remainingOperators.dequeue()
        println("dequeue:" + op)

        val leftParent = remainingParents.filter(p => op.leftJoinSet.subsetOf(p.outputSchema.getGlobalIdSet)).head

        val rightParent = remainingParents
          .filter(p => op.rightJoinSet.subsetOf(p.outputSchema.getGlobalIdSet) && p != leftParent).head

        op.setParents(leftParent, rightParent)
        remainingParents -= leftParent
        remainingParents -= rightParent
        remainingParents += op
        build(remainingParents, remainingOperators)
      }
    }

    val remainingParents = mutable.Set[Operator]()
    parentOperators.foreach(remainingParents += _)
    tailOperator = build(remainingParents, innerJoinOperators.clone()).head
    outputSchema = tailOperator.outputSchema
  }



  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    tailOperator.execute(exec)
  }

  def replaceParent(oldParent: Operator, newParent: Operator) {
    parentOperators -= oldParent
    parentOperators += newParent
  }

  override def toString = {
    def toStrRecursive(op : Operator, depth : Int = 0) : String = {
      if(innerJoinOperators.exists(_ == op))
        depth + ":" + op.toString + "\n" +
          op.parentOperators.map(p => toStrRecursive(p, depth + 1)).reduce(_ + "\n" + _)
      else
        ""
    }
    super.toString + "\n" + toStrRecursive(tailOperator)
  }
}


class WhereOperatorSet(parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  val operators = mutable.PriorityQueue[WhereOperator]()(WhereOperatorOrdering)
  var tailOperator: Operator = null

  def WhereOperatorOrdering = new Ordering[WhereOperator]{
    def compare(a : WhereOperator, b : WhereOperator) = b.selectivity.compare(a.selectivity)
  }

  def addWhereOperator(op : WhereOperator){
    operators += op
    if(operators.size == 1){
      outputSchema = op.outputSchema
    }
  }

  def optimize(){
    def built(parent : Operator, remaining :  mutable.PriorityQueue[WhereOperator]) : Operator = {
      if(remaining.size == 0)
        parent
      else{
        val current = remaining.dequeue()
        current.setParent(parent)
        built(current, remaining)
      }
    }

    tailOperator = built(parentOperators.head, operators.clone())
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    tailOperator.execute(exec)
  }

  override def toString = {
    def toStrRecursive(op : Operator) : String = {
      if(operators.exists(_ == op))
        op.toString + "\n" + toStrRecursive(op.parentOperators.head)
      else
        ""
    }
    super.toString + "\n" + toStrRecursive(tailOperator)
  }

}

abstract class UnaryOperator extends Operator{
  def setParent(parentOp : Operator) {
    if(parentOperators.size > 0){
      parentOperators.foreach(parent => parent.childOperators -= this)
      parentOperators.clear()
    }
    parentOperators += parentOp
    parentOp.childOperators += this
  }

  override def replaceParent(oldParent : Operator, newParent : Operator){
    setParent(newParent)
  }
}

abstract class BinaryOperator extends Operator{
  def setParents(parentOp1 : Operator, parentOp2 : Operator){
    if(parentOperators.size > 0){
      parentOperators.foreach(p => p.childOperators -= this)
      parentOperators.clear()
    }
    parentOperators += parentOp1
    parentOperators += parentOp2
    parentOp1.childOperators += this
    parentOp2.childOperators += this
  }

  override def replaceParent(oldParent : Operator, newParent : Operator){
    if(parentOperators(0) == oldParent)
      setParents(newParent, parentOperators(1))
    else if(parentOperators(1) == oldParent)
      setParents(parentOperators(0), newParent)
    else
      throw new Exception("The original parent does not exist")
  }
}

class WindowOperator(parentOp : Operator, val batches : Int, parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  setParent(parentOp)
  var cached = Map[Time,RDD[IndexedSeq[Any]]]()

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    outputSchema = parentOp.outputSchema
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    val resultFromParent = parentOperators.head.execute(exec).persist(this.parentCtx.defaultStorageLevel)

    cached += exec.getTime -> resultFromParent
    //cached.foreach(kvp => if( !(kvp._1 > exec.getTime - this.parentCtx.getBatchDuration * batches)) kvp._2.foreach(_.unpersist(false)))
    cached = cached.filter(kvp => kvp._1 > exec.getTime - this.parentCtx.getBatchDuration * batches)

    this.parentCtx.ssc.sparkContext.union( cached.values.toSeq).coalesce(this.parentCtx.ssc.sparkContext.defaultParallelism, false)

  }

}


class SelectOperator(parentOp : Operator,
                     selectColGlobalId : IndexedSeq[Int],
                     parentCtx : SqlSparkStreamingContext) extends UnaryOperator {

  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var isSelectAll = false
  var localColId = IndexedSeq[Int]()
  setParent(parentOp)
  outputSchema = new Schema(selectColGlobalId
    .map(gid => (parentOperators.head.outputSchema.getClassFromGlobalId(gid),gid)))


  def getSelectColGlobalId = selectColGlobalId

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    isSelectAll = selectColGlobalId.toSeq.equals(parentOperators.head.outputSchema.getLocalIdFromGlobalId.keySet)
    localColId = selectColGlobalId.map(gid => parentOp.outputSchema.getLocalIdFromGlobalId(gid))
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    val rdd = parentOperators.head.execute(exec)
    if(isSelectAll)
      rdd
    else{
      val localColId = this.localColId
      //println("print in select op")
      //SqlHelper.printRDD(rdd)
      rdd.map(record => localColId.map(id => record(id)) )
    }

  }

  override def toString = super.toString + selectColGlobalId
}

class WhereOperator(parentOp : Operator,
                    func : (IndexedSeq[Any], Schema) => Boolean, whereColumnId : Set[Int],
                    parentCtx : SqlSparkStreamingContext) extends UnaryOperator{

  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  setParent(parentOp)



  var selectivity = 1.0

  def getWhereColumnId = whereColumnId

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    outputSchema = parentOp.outputSchema
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    val func = this.func
    val outputSchema = this.outputSchema

    val rdd = parentOperators.head.execute(exec)

    rdd.filter(record => func(record, outputSchema))
  }

  override def toString = super.toString + whereColumnId + "Sel:" + selectivity
}

class GroupByCombiner(createCombiner : Any,
                      mergeValue : Any,
                      mergeCombiners : Any,
                      finalProcessing : Any,
                      resultType : String) extends Serializable{

  def getCreateCombiner = createCombiner.asInstanceOf[Any=>Any]
  def getMergeValue = mergeValue.asInstanceOf[(Any,Any)=>Any]
  def getMergeCombiners = mergeCombiners.asInstanceOf[(Any,Any)=>Any]
  def getFinalProcessing = finalProcessing.asInstanceOf[Any=>Any]
  def getResultType = resultType
}

class GroupByOperator(parentOp : Operator,
                      keyColumnsArr : IndexedSeq[Int],
                      functions : Map[Int, GroupByCombiner],
                      parentCtx : SqlSparkStreamingContext) extends UnaryOperator{

  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)

  var windowSize = 1

  val valueColumns = functions.keySet
  val keyColumns = keyColumnsArr.toSet
  assert(keyColumns.intersect(valueColumns).isEmpty)
  setParent(parentOp)
  val getOldGIDFromNewGID = functions.map(tp => (sqlContext.columns.getGlobalColId, tp._1)).toMap
  val getNewGIDFromOldGID = getOldGIDFromNewGID.map(tp => (tp._2,tp._1)).toMap

  outputSchema = new Schema(keyColumnsArr.map(gid => (parentOp.outputSchema.getClassFromGlobalId(gid),gid))
    ++ functions.map(kvp => (kvp._2.getResultType, getNewGIDFromOldGID(kvp._1))))

  var cached = Map[Time,RDD[(IndexedSeq[Any],IndexedSeq[Any])]]()


  def getKeyColumnsArr = keyColumnsArr
  def getFunctions = functions

  var partitioner = new HashPartitioner(parentCtx.ssc.sparkContext.defaultParallelism)

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    assert(keyColumns.union(valueColumns).subsetOf(parentOp.outputSchema.getSchemaArray.map(_._2).toSet))
  }

  def setWindow(newWindowSize : Int){
    this.windowSize = newWindowSize
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    val createCombiner = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getCreateCombiner(x(kvp._1))).toIndexedSeq
    }

    val mergeValue = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeValue(x(kvp._1), y(kvp._1))).toIndexedSeq
    }

    val mergeCombiners = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeCombiners(x(kvp._1), y(kvp._1))).toIndexedSeq
    }

    val finalProcessing = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getFinalProcessing(x(kvp._1))).toIndexedSeq
    }

    val localKeyColumnArr = this.keyColumnsArr.map(parentOp.outputSchema.getLocalIdFromGlobalId(_))
    val localFunctions = this.functions.map(kvp => (parentOp.outputSchema.getLocalIdFromGlobalId(kvp._1), kvp._2) )
    val localValueFunctions = localFunctions.zipWithIndex.map(kvp => (kvp._2,kvp._1._2))//the location in the groupby columns

    val rdd = parentOperators.head.execute(exec)



    if(windowSize == 1){
      val kvpRdd = rdd.map(record => (
        localKeyColumnArr.map(record(_)).toIndexedSeq,
        localFunctions.map(kvp => record(kvp._1)).toIndexedSeq)
      )

      kvpRdd.combineByKey[IndexedSeq[Any]](
        (x : IndexedSeq[Any]) => createCombiner(x,localValueFunctions),
        (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeValue(x,y,localValueFunctions),
        (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeCombiners(x,y,localValueFunctions),
        partitioner
      ).mapValues(finalProcessing(_,localValueFunctions))
        .map(kvp => kvp._1 ++ kvp._2)
    }else{
      val grouped = groupBy(rdd)

      cached += exec.getTime -> grouped
      cached = cached.filter(tp => tp._1 > exec.getTime - this.parentCtx.getBatchDuration * windowSize)

      val unioned =  new PartitionerAwareUnionRDD(cached.values.head.sparkContext, cached.values.toArray.toSeq)

      unioned.reduceByKey((x,y) => mergeCombiners(x,y,localValueFunctions))
        .mapValues(records => finalProcessing(records, localValueFunctions))
        .map(tp => tp._1 ++ tp._2)

    }

  }

  //TODO: Try to keep the partition info by avoiding map
  def groupBy(rdd : RDD[IndexedSeq[Any]]) : RDD[(IndexedSeq[Any],IndexedSeq[Any])] = {
    val createCombiner = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getCreateCombiner(x(kvp._1))).toIndexedSeq
    }

    val mergeValue = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeValue(x(kvp._1), y(kvp._1))).toIndexedSeq
    }

    val mergeCombiners = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeCombiners(x(kvp._1), y(kvp._1))).toIndexedSeq
    }


    val localKeyColumnArr = this.keyColumnsArr.map(parentOp.outputSchema.getLocalIdFromGlobalId(_))
    val localFunctions = this.functions.map(kvp => (parentOp.outputSchema.getLocalIdFromGlobalId(kvp._1), kvp._2) )
    val localValueFunctions = localFunctions.zipWithIndex.map(kvp => (kvp._2,kvp._1._2))//the location in the groupby columns

    val kvpRdd = rdd.map(record => (
      localKeyColumnArr.map(record(_)).toIndexedSeq,
      localFunctions.map(kvp => record(kvp._1)).toIndexedSeq)
    )


    kvpRdd.combineByKey[IndexedSeq[Any]](
      (x : IndexedSeq[Any]) => createCombiner(x,localValueFunctions),
      (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeValue(x,y,localValueFunctions),
      (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeCombiners(x,y,localValueFunctions),
      partitioner
    ).asInstanceOf[RDD[(IndexedSeq[Any],IndexedSeq[Any])]]
//    val reduced = combined.mapValues(finalProcessing(_,localValueFunctions))
//    val rr = reduced.map(kvp => kvp._1 ++ kvp._2)
//    rr.map(arr => arr.toIndexedSeq)
  }

//  def mergeBatch(rdd : RDD[(IndexedSeq[Any],IndexedSeq[Any])]) : RDD[IndexedSeq[Any]] = {
//
//    val mergeCombiners = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
//      func.map(kvp => kvp._2.getMergeCombiners(x(kvp._1), y(kvp._1))).toIndexedSeq
//    }
//
//    val finalProcessing = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
//      func.map(kvp => kvp._2.getFinalProcessing(x(kvp._1))).toIndexedSeq
//    }
//
//    val localFunctions = this.functions.map(kvp => (parentOp.outputSchema.getLocalIdFromGlobalId(kvp._1), kvp._2) )
//    val localValueFunctions = localFunctions.zipWithIndex.map(kvp => (kvp._2,kvp._1._2))//the location in the groupby columns
//
//    rdd.reduceByKey((x,y) => mergeCombiners(x,y,localValueFunctions))
//      .map(kvp => kvp._1 ++ finalProcessing(kvp._2,localValueFunctions))
//
//  }

  override def toString = super.toString + "keys:" + keyColumnsArr + " values:" + functions + " W:" + windowSize


}


class ParseOperator(schema : Schema,
                    delimiter : String,
                    inputStreamName : String,
                    parentCtx : SqlSparkStreamingContext) extends UnaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  outputSchema = schema

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {

    val outputSchema = this.outputSchema
    val delimiter = this.delimiter
    val parseLine =  (line : String) => {
      val parse = (str : String, tp : String) => {
        tp match{
          case "int" => str.toInt
          case "double" => str.toDouble
          case "string" => str
          case _ => throw new Exception("unknown data type")
        }
      }

      val lineArr = line.trim.split(delimiter).toIndexedSeq

      if (lineArr.length != outputSchema.getSchemaArray.length){
        IndexedSeq[Any]()
      }
      else
        try{
          lineArr.zipWithIndex.map(entry => parse(entry._1, outputSchema.getSchemaArray(entry._2)._1 ))
        }catch{
          case e : Exception => IndexedSeq[Any]()
        }
    }

    val rdd = exec.getInputRdds(inputStreamName)

    val returnRDD = rdd.map(line => parseLine(line)).filter(line => line.length > 0)

    returnRDD
  }

  override def setParent(parentOp : Operator){}

  override def toString = super.toString + schema
}


class OutputOperator(parentOp : Operator,
                     selectColGlobalId : IndexedSeq[Int],
                     parentCtx : SqlSparkStreamingContext) extends UnaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var isSelectAll = false
  var localColId = IndexedSeq[Int]()
  setParent(parentOp)
  outputSchema = new Schema(
    selectColGlobalId.map(gid => (parentOperators.head.outputSchema.getClassFromGlobalId(gid),gid))
  )

  override def setParent(parentOp : Operator){

    super.setParent(parentOp)
    isSelectAll = selectColGlobalId.toSeq.equals(parentOperators.head.outputSchema.getLocalIdFromGlobalId.keySet)
    localColId = selectColGlobalId.map(gid => parentOp.outputSchema.getLocalIdFromGlobalId(gid))
  }

  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {
    val rdd = parentOperators.head.execute(exec)




    if(isSelectAll)
      rdd
    else{
      val localColId = this.localColId
      //println("print in select op")
      //SqlHelper.printRDD(rdd)
      rdd.map(record => localColId.map(id => record(id)) )
    }


  }
}


class InnerJoinOperator(parentOp1 : Operator,
                        parentOp2 : Operator,
                        joinCondition : IndexedSeq[(Int, Int)],
                        parentCtx : SqlSparkStreamingContext
                         ) extends BinaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var getLocalIdFromGlobalId : Broadcast[Map[Int,Int]] = null
  var localJoinCondition : Broadcast[IndexedSeq[(Int,Int)]] = null
  var broadcastSchema : Broadcast[Schema] = null
  setParents(parentOp1, parentOp2)
  val leftJoinSet = joinCondition.map(tp=>tp._1).toSet
  val rightJoinSet = joinCondition.map(tp=>tp._2).toSet

  var leftWindowSize = 1
  var rightWindowSize = 1


  var cached = Map[(Time, Time), RDD[IndexedSeq[Any]]]()

  var leftShuffleCache = Map[Time, RDD[(IndexedSeq[Any],IndexedSeq[Any])]]()
  var rightShuffleCache =  Map[Time, RDD[(IndexedSeq[Any],IndexedSeq[Any])]]()

  var selectivity : Double = 1.0

  var partitioner = new HashPartitioner(parentCtx.ssc.sparkContext.defaultParallelism)

  def setLeftWindow(size : Int){
    leftWindowSize = size
  }

  def setRightWindow(size : Int){
    rightWindowSize = size
  }

  override def setParents(parentOp1 : Operator, parentOp2 : Operator){
    super.setParents(parentOp1, parentOp2)

    joinCondition.foreach(tp =>
      assert(parentOp1.outputSchema.getClassFromGlobalId(tp._1) == parentOp2.outputSchema.getClassFromGlobalId(tp._2))
    )

    outputSchema = new Schema(
      parentOp1.outputSchema.getSchemaArray.toSet.union(parentOp2.outputSchema.getSchemaArray.toSet).toArray.sortBy(_._2)
    )

    val left = parentOp1.outputSchema.getLocalIdFromGlobalId
    val right = parentOp2.outputSchema.getLocalIdFromGlobalId.map(kvp => (kvp._1, kvp._2 + left.size))
    getLocalIdFromGlobalId = this.parentCtx.ssc.sparkContext.broadcast(left ++ right)

    localJoinCondition = this.parentCtx.ssc.sparkContext.broadcast(joinCondition.map(tp =>
      (parentOperators(0).outputSchema.getLocalIdFromGlobalId(tp._1),
        parentOperators(1).outputSchema.getLocalIdFromGlobalId(tp._2))
    ))

    broadcastSchema = this.parentCtx.ssc.sparkContext.broadcast(outputSchema)
  }


  val convertToHashMap = (iter : Iterator[(IndexedSeq[Any],IndexedSeq[Any])]) => {
    {

      val multiMap = new mutable.HashMap[IndexedSeq[Any], ArrayBuffer[IndexedSeq[Any]]]

      iter.foreach(kvp =>
        if(multiMap.contains(kvp._1))
          multiMap(kvp._1).append(kvp._2)
        else
          multiMap += kvp._1 -> ArrayBuffer(kvp._2)
      )

      new Iterator[mutable.HashMap[IndexedSeq[Any], ArrayBuffer[IndexedSeq[Any]]]](){
        val content = multiMap
        var _hasNext = true
        override def hasNext() = _hasNext
        override def next() = {
          if(_hasNext){
            _hasNext = false
            content
          }else
            throw new NoSuchElementException
        }

      }
    }
  }


  override def execute(exec : Execution) : RDD[IndexedSeq[Any]] = {


    val localJoinCondition = this.localJoinCondition

    val leftParentResult = parentOperators(0).execute(exec)
      .map(record => (localJoinCondition.value.map(tp => record(tp._1)),record))
      .partitionBy(this.partitioner)
      .persist(this.parentCtx.defaultStorageLevel)

    val rightParentResult = parentOperators(1).execute(exec)
      .map(record => (localJoinCondition.value.map(tp => record(tp._1)),record))
      .partitionBy(this.partitioner)
      .persist(this.parentCtx.defaultStorageLevel)

    leftShuffleCache += exec.getTime -> leftParentResult
    rightShuffleCache += exec.getTime -> rightParentResult

    leftShuffleCache = leftShuffleCache.filter(tp => tp._1 > exec.getTime - this.parentCtx.getBatchDuration * leftWindowSize)
    rightShuffleCache = rightShuffleCache.filter(tp => tp._1 > exec.getTime - this.parentCtx.getBatchDuration * rightWindowSize)

    var result = Map[(Time, Time), RDD[IndexedSeq[Any]]]()

    for((leftTime, leftRDD) <- leftShuffleCache; (rightTime, rightRDD) <- rightShuffleCache){
      if(cached.contains((leftTime, rightTime))){
        result += (leftTime,rightTime) -> cached((leftTime,rightTime))
      }else{
        val getStat =
          if(leftTime == exec.getTime && rightTime == exec.getTime)
            true
          else
            false
        val joined = join(leftShuffleCache(leftTime), rightShuffleCache(rightTime), getStat)
          .persist(this.parentCtx.defaultStorageLevel)
        result += (leftTime,rightTime) -> joined

      }
    }

    cached = result

    val finalRDD = new PartitionerAwareUnionRDD(cached.values.head.sparkContext, cached.values.toArray.toSeq)

    //val finalRDD = this.parentCtx.ssc.sparkContext.union(cached.values.toArray.toSeq)
    //  .coalesce(this.parentCtx.ssc.sparkContext.defaultParallelism, false)

    //logInfo(finalRDD.toDebugString)

    finalRDD
  }

  def hashedJoin(left : RDD[mutable.HashMap[IndexedSeq[Any], ArrayBuffer[IndexedSeq[Any]]]],
                 right : RDD[mutable.HashMap[IndexedSeq[Any], ArrayBuffer[IndexedSeq[Any]]]]) = {
    left.zipPartitions(right)((leftIter, rightIter) => {
      val leftTable = leftIter.next
      val rightTable = rightIter.next

      leftTable.flatMap(kvp => {
        if(rightTable.contains(kvp._1)){
          kvp._2.flatMap(leftRecord => rightTable(kvp._1).map(rightRecord => leftRecord ++ rightRecord))
        }else{
          Seq()
        }
      }).iterator
    })
  }


  def join(leftPartitioned : RDD[(IndexedSeq[Any],IndexedSeq[Any])],
           rightPartitioned : RDD[(IndexedSeq[Any],IndexedSeq[Any])], getStat : Boolean = false) = {

    val getLocalIdFromGlobalId = this.getLocalIdFromGlobalId
    val outputSchema = this.outputSchema



    val joined = leftPartitioned.join(rightPartitioned)
    val result = joined.mapPartitions (it => it.map{pair =>
      val combined = pair._2._1 ++ pair._2._2
      outputSchema.getSchemaArray.map(kvp => combined(getLocalIdFromGlobalId.value(kvp._2)))
    }, true
    )


    if(getStat && this.parentCtx.args.contains("-reorder")){
//      leftPartitioned.persist(this.parentCtx.defaultStorageLevel)
//      rightPartitioned.persist(this.parentCtx.defaultStorageLevel)
//      result.persist(this.parentCtx.defaultStorageLevel)



      getSelectivityActor ! (leftPartitioned,rightPartitioned, result)
    }


    result
  }


  val getSelectivityActor = actor{
    while(true){
      try{
        receive{
          case (rdd1 : RDD[Any], rdd2 :RDD[Any], joined : RDD[Any]) =>
          {
            val joinAcc = parentCtx.ssc.sparkContext.accumulator(0L)
            val rdd1Acc = parentCtx.ssc.sparkContext.accumulator(0L)
            val rdd2Acc = parentCtx.ssc.sparkContext.accumulator(0L)

            joined.foreach(l => joinAcc += 1)
            rdd1.foreach(l => rdd1Acc += 1)
            rdd2.foreach(l => rdd2Acc += 1)

            val joinedSize = joinAcc.value
            val rdd1Size = rdd1Acc.value
            val rdd2Size = rdd2Acc.value
            if(rdd1Size > 0 && rdd2Size > 0)
            {
              selectivity = joinedSize.toDouble /(rdd1Size * rdd2Size)
            }
          }
        }
      }catch{
        case e : Exception => e.printStackTrace()
      }
    }
  }

  def getJoinCondition = joinCondition

  override def toString = super.toString + joinCondition + " Sel:" + selectivity + " W:" + leftWindowSize + " " + rightWindowSize
}

//
//class PartitionerAwareUnionRDDPartition(val idx: Int, val partitions: Array[Partition])
//  extends Partition {
//  override val index = idx
//  override def hashCode(): Int = idx
//}
//
//class PartitionerAwareUnionRDD[T: ClassManifest](
//    sc: SparkContext,
//    var rdds: Seq[RDD[T]]
//  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
//  require(rdds.length > 0)
//  require(rdds.flatMap(_.partitioner).toSet.size == 1,
//    "Parent RDDs do not have the same partitioner: " + rdds.flatMap(_.partitioner))
//
//  override val partitioner = rdds.head.partitioner
//
//  override def getPartitions: Array[Partition] = {
//    val numPartitions = rdds.head.partitions.length
//    (0 until numPartitions).map(index => {
//      val parentPartitions = rdds.map(_.partitions(index)).toArray
//      new PartitionerAwareUnionRDDPartition(index, parentPartitions)
//    }).toArray
//  }
//  /*
//  // Get the location where most of the partitions of parent RDDs are located
//  override def getPreferredLocations(s: Partition): Seq[String] = {
//    logDebug("Getting preferred locations for " + this)
//    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
//    val locations = rdds.zip(parentPartitions).flatMap {
//      case (rdd, part) => {
//        val parentLocations = currPrefLocs(rdd, part)
//        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
//        parentLocations
//      }
//    }
//
//    if (locations.isEmpty) {
//      Seq.empty
//    } else  {
//      Seq(locations.groupBy(x => x).map(x => (x._1, x._2.length)).maxBy(_._2)._1)
//    }
//  }
//  */
//  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
//    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
//    rdds.zip(parentPartitions).iterator.flatMap {
//      case (rdd, p) => rdd.iterator(p, context)
//    }
//  }
//  /*
//  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
//  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
//    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
//  }
//  */
//}