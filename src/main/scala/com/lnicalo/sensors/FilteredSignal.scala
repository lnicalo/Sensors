package com.lnicalo.sensors
import scala.language.implicitConversions
import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

class FilteredSignal[K: ClassTag, V: ClassTag](val parent: RDD[(K, List[List[(Double, V)]])])
  extends RDD[(K, List[List[(Double, V)]])](parent) {

  type Ops = (List[List[(Double, V)]]) => HashMap[String, Any]
  var ops = List[Ops]()

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[List[(Double, V)]])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def toActivations() = {
    val rdd = parent.flatMap { case (key, value) =>
      for (s <- value) yield ((key, (s.head._1, s.last._1)), s)
    }
    Signal[(K, (Double, Double)), V](rdd)
  }

  def addOp(f: Ops): FilteredSignal[K, V] = {
    this.ops ::= f
    this
  }

  // Operations
  def timings(): FilteredSignal[K, V] = this.addOp(FilteredSignal.timings)

  def lastValue(): FilteredSignal[K,V] = this.addOp(FilteredSignal.last)

  def firstValue(): FilteredSignal[K,V] = this.addOp(FilteredSignal.first)

  def toDataset = {
    val rdd = parent.mapValues { x =>
      var r = HashMap[String, Any]()
      for(op <- ops) {
        val out = op(x)
        out.foreach(x => r = r.updated(x._1, x._2))
      }
      r
    }
    rdd.collectAsMap()
  }
}

object FilteredSignal {
  def timings[V](x: List[List[(Double, V)]]): HashMap[String, Option[Double]] = {
    var out = HashMap[String, Option[Double]]("Start" -> None, "End" -> None, "Duration" -> None)
    if (!x.isEmpty) {
      val head = x.head
      val last = x.last
      if (!head.isEmpty) {
        out = out.updated("Start", Some(head.head._1))
      }
      if (!last.isEmpty) {
        out = out.updated("End", Some(last.last._1))
      }
      if (!head.isEmpty && !last.isEmpty) {
        out = out.updated("Duration", Some(last.last._1 - head.head._1))
      }
    }
    out
  }

  def last[V](x: List[List[(Double, V)]]): HashMap[String, Option[V]] = {
    val tmp = x.last
    if (tmp.length > 1) {
      HashMap("Last" -> Some(tmp(tmp.length - 2)._2))
    }
    else {
      if (tmp.length == 1 ) {
        HashMap("Last" -> Some(tmp.last._2))
      }
      else {
        HashMap("Last" -> None)
      }
    }
  }

  def first[V](x: List[List[(Double, V)]]): HashMap[String, Option[V]] =  {
    val tmp = x.head
    if (tmp.isEmpty) {
      HashMap("First" -> None)
    }
    else {
      HashMap("First" -> Some(tmp.head._2))
    }
  }

  /*implicit def signalToStringSignalFunctions[K] (filteredSignal: FilteredSignal[K, String])
                                                (implicit kt: ClassTag[K]):
    StringSignalFunctions[(K, (Double, Double))] = {
      new StringSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToMathSignalFunctions[K, V: Fractional] (filteredSignal: FilteredSignal[K, V])
                                                             (implicit kt: ClassTag[K], vt: ClassTag[V]):
     MathSignalFunctions[(K, (Double, Double)), V] = {
      new MathSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToBooleanSignalFunctions[K] (filteredSignal: FilteredSignal[K, Boolean])
                                                (implicit kt: ClassTag[K]):
  BooleanSignalFunctions[(K, (Double, Double))] = {
    new BooleanSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToSignal[K, V] (filteredSignal: FilteredSignal[K, V])
                                    (implicit kt: ClassTag[K], vt: ClassTag[V]):
  Signal[(K, (Double, Double)), V] = {
    filteredSignal.flatten()
  }*/
}

