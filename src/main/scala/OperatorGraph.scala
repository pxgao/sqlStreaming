package main.scala

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 10/16/13
 * Time: 9:15 PM
 * To change this template use File | Settings | File Templates.
 */
class OperatorGraph(_parentCtx : SqlSparkStreamingContext) extends Logging {
  val parentCtx = _parentCtx

  val allOperators = scala.collection.mutable.ArrayBuffer[Operator]()


  val outputOperators = scala.collection.mutable.ArrayBuffer[OutputOperator]()
  val windowOperators = scala.collection.mutable.ArrayBuffer[WindowOperator]()
  val groupbyOperators = scala.collection.mutable.ArrayBuffer[GroupByOperator]()

  val whereOperators = scala.collection.mutable.ArrayBuffer[WhereOperator]()
  val whereOperatorSets = ArrayBuffer[WhereOperatorSet]()

  val innerJoinOperators = ArrayBuffer[InnerJoinOperator]()
  val innerJoinOperatorSets = ArrayBuffer[InnerJoinOperatorSet]()



  def addOperator(operator : Operator){
    allOperators += operator
    operator match{
      case operator : OutputOperator => outputOperators += operator
      case operator : WhereOperator => whereOperators += operator
      case operator : InnerJoinOperator => innerJoinOperators += operator
      case operator : WindowOperator => windowOperators += operator
      case operator : GroupByOperator => groupbyOperators += operator
      case _ => Unit
    }
  }

  def groupPredicate(){
    val visited = scala.collection.mutable.HashSet[Operator]()
    def visit(op : Operator, set : Option[WhereOperatorSet]){
      println("calling visit" + op)
      if(visited(op))
        return
      visited += op


      if(op.isInstanceOf[WhereOperator] && op.childOperators.size < 2) {
        if(set.isEmpty){
          val newSet = new WhereOperatorSet(parentCtx)
          newSet.addWhereOperator(op.asInstanceOf[WhereOperator])
          whereOperatorSets += newSet
          op.childOperators.foreach(c => c.replaceParent(op,newSet))
          op.parentOperators.foreach(p => visit(p,Some(newSet)))
        }else{
          set.get.addWhereOperator(op.asInstanceOf[WhereOperator])
          op.parentOperators.foreach(p => visit(p,set))
        }
      }else{
        if(!set.isEmpty){
          set.get.setParent(op)
        }
        op.parentOperators.foreach(p => visit(p, None))
      }
    }
    outputOperators.foreach(op => visit(op, None))
    println("finish")
  }

  def groupInnerJoin(){
    val visited = scala.collection.mutable.HashSet[Operator]()
    def visit(op : Operator, set : Option[InnerJoinOperatorSet]){
      if(visited(op))
        return
      visited += op

      if(op.isInstanceOf[InnerJoinOperator] && op.childOperators.size < 2){
        if(set.isEmpty){
          val newSet = new InnerJoinOperatorSet(parentCtx)
          newSet.addInnerJoinOperator(op.asInstanceOf[InnerJoinOperator])
          innerJoinOperatorSets += newSet
          op.childOperators.foreach(c => c.replaceParent(op, newSet))
          op.parentOperators.foreach(p => visit(p, Some(newSet)))
        }else{
          set.get.addInnerJoinOperator(op.asInstanceOf[InnerJoinOperator])
          op.parentOperators.foreach(p => visit(p,set))
        }
      }else{
        if(!set.isEmpty){
          set.get.addParent(op)
        }
        op.parentOperators.foreach(p => visit(p, None))
      }
    }
    outputOperators.foreach(op => visit(op, None))
  }


  def pushAllPredicates = whereOperators.foreach(w => pushPredicate(w))

  def pushPredicate(operator : WhereOperator){

    var parentToReplaceWhenBinary : Operator = null

    def findPushTo(pushTo : Operator) : Operator = {
      val p =
        if(pushTo.isInstanceOf[UnaryOperator])
          pushTo.parentOperators.head
        else if(pushTo.isInstanceOf[BinaryOperator])
          if(pushTo.isInstanceOf[InnerJoinOperator]){
            parentToReplaceWhenBinary =
              if(operator.getWhereColumnId.subsetOf(pushTo.asInstanceOf[InnerJoinOperator].parentOperators(0).outputSchema.getLocalIdFromGlobalId.keySet))
                pushTo.parentOperators(0)
              else if(operator.getWhereColumnId.subsetOf(pushTo.asInstanceOf[InnerJoinOperator].parentOperators(1).outputSchema.getLocalIdFromGlobalId.keySet))
                pushTo.parentOperators(1)
              else
                throw new Exception()
            parentToReplaceWhenBinary
          }

      if(p.asInstanceOf[Operator].getChildOperators.size > 1)
        return pushTo
      p match{
        case p:WhereOperator => findPushTo(p)
        case p:SelectOperator => findPushTo(p)
        case p:GroupByOperator => {
          val gb = p.asInstanceOf[GroupByOperator]
          if(operator.getWhereColumnId.subsetOf(gb.getKeyColumnsArr.toSet)){
            findPushTo(p)
          }else{
            return pushTo
          }
        }
        case p:ParseOperator => return pushTo
        case p:InnerJoinOperator => {
          if(operator.getWhereColumnId.subsetOf(p.asInstanceOf[InnerJoinOperator].parentOperators(0).outputSchema.getLocalIdFromGlobalId.keySet) ||
            operator.getWhereColumnId.subsetOf(p.asInstanceOf[InnerJoinOperator].parentOperators(1).outputSchema.getLocalIdFromGlobalId.keySet) ){
            findPushTo(p)
          }else{
            return pushTo
          }
        }
      }
    }

    var pushTo : Operator = findPushTo(operator)

    if(pushTo == operator)
      return

    operator.childOperators.foreach(op => op.replaceParent(operator, operator.parentOperators.head))

    val oldParent = pushTo match{
      case pushTo : UnaryOperator => pushTo.parentOperators.head
      case pushTo : BinaryOperator => parentToReplaceWhenBinary
    }

    operator.setParent(oldParent)
    pushTo.replaceParent(oldParent,operator)

  }

  def pushWindow(windowOp : WindowOperator){

    //this return the operator we can push upto (above)
    def findPushTo(pushTo : Operator, parent : Operator) : (Operator, Operator) = {
      if(pushTo.getChildOperators.size != 1)
        return (pushTo, parent)
      else{
        pushTo match{
          case pushTo : GroupByOperator => return (pushTo, parent)
          case pushTo : InnerJoinOperator => return (pushTo, parent)
          case pushTo : OutputOperator => return (pushTo, parent)
          case pushTo : ParseOperator => throw new Exception("Impossible to have parse operator here")
          case pushTo : SelectOperator => findPushTo(pushTo.getChildOperators.head, pushTo)
          case pushTo : WhereOperator => findPushTo(pushTo.getChildOperators.head, pushTo)
          case pushTo : WindowOperator => findPushTo(pushTo.getChildOperators.head, pushTo)
        }
      }



    }

    val (pushTo, parent) = findPushTo(windowOp, windowOp.parentOperators.head)

    logDebug("PushTo:" + pushTo)


    if(pushTo == windowOp)
      return

    windowOp.childOperators.foreach( op => op.replaceParent(windowOp, windowOp.parentOperators.head))

    windowOp.setParent(parent)
    pushTo.replaceParent(parent, windowOp)




  }

  def pushAllWindows = windowOperators.foreach(w => pushWindow(w))

  def incrementalGroupBy = {
    groupbyOperators.foreach(gb => {
      gb.parentOperators.head match{
        case parent : WindowOperator =>{
          val windowSize = parent.batches
          gb.setWindow(windowSize)
          gb.replaceParent(parent, parent.parentOperators.head)
          allOperators -= parent
          windowOperators -= parent
        }
        case _ => Unit
      }

    })
  }


  override def toString() : String = {
    val buffer = new StringBuilder
    outputOperators.foreach(op => buffer.append(op.getFamilyTree(0)))
    buffer.toString()
  }

  def execute(func : RDD[IndexedSeq[Any]] => Unit, exec : Execution){
    outputOperators.foreach(op => func(op.execute(exec)))
  }
}


