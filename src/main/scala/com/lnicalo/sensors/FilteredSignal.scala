package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class FilteredSignal[K, V: Fractional](val parent: RDD[(K, List[List[(Double, V)]])])
                                      (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, List[List[(Double, V)]])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[List[(Double, V)]])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions
}

