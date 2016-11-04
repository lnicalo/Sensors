package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class FilteredSignal[K: ClassTag, V: Fractional: ClassTag](val parent: RDD[(K, List[List[(Double, V)]])])
  extends RDD[(K, List[List[(Double, V)]])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[List[(Double, V)]])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def flatten() = {
    val rdd = this.flatMap { case (key, value) =>
      for (s <- value) yield ((key, (s.head._1, s.last._1)), s)
    }
    Signal[(K, (Double, Double)), V](rdd)
  }
}

