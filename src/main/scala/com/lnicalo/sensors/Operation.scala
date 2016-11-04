package com.lnicalo.sensors

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 04/11/2016.
  */
abstract class Operation[K: ClassTag, M: ClassTag] (parent: RDD[(K, HashMap[String, M])])
  extends RDD[(K, HashMap[String, M])](parent){
  def compute(split: Partition, context: TaskContext): Iterator[(K, HashMap[String, M])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

}
