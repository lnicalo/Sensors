package com.lnicalo.sensors

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.collection.immutable.HashMap
/**
  * Created by LNICOLAS on 01/12/2016.
  */
class Dataset[K: ClassTag](val parent: RDD[(K, HashMap[String, Any])])
  extends RDD[(K, HashMap[String, Any])](parent) {

  override def compute(split: Partition, context: TaskContext): Iterator[(K, HashMap[String, Any])] =
    parent.iterator(split, context)

  protected override def getPartitions: Array[Partition] = parent.partitions

}
