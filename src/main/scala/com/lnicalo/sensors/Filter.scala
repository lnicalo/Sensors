package com.lnicalo.sensors

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Filter[K: ClassTag](val parent: RDD[(K, List[(Double, Boolean)])])
  extends RDD[(K, List[(Double, Boolean)])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, Boolean)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def applyPairWiseOperation(that: Filter[K])(op: (Boolean, Boolean) => Boolean) =
    new Filter[K](this.join(that)
      .mapValues{ case (v, w) => Signal.PairWiseOperation[Boolean,Boolean,Boolean](v, w)(op) })

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Boolean operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  def &&(that: Filter[K]) = applyPairWiseOperation(that) { _ && _ }
  def ||(that: Filter[K]) = applyPairWiseOperation(that) { _ || _ }
  def and(that: Filter[K]) = &&(that)
  def or(that: Filter[K]) = ||(that)
  def unary_! = new Filter[K](parent.mapValues(x => x.map(v => (v._1, !v._2))))
}

object Filter {
  def apply[K: ClassTag](local: Array[(K, List[(Double, Boolean)])])
                        (implicit sc: SparkContext) =
    new Filter[K](sc.parallelize(local))
}


