package com.lnicalo.sensors

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 02/11/2016.
  */
class Partitioner[K](val parent: RDD[(K, List[(Double, Boolean)])])
                              (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, List[(Double, Boolean)])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, Boolean)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def applyPairWiseOperation(that: Partitioner[K])(op: (Boolean, Boolean) => Boolean) =
    new Partitioner[K](this.join(that).mapValues { case (v, w) => Signal.PairWiseOperation[Boolean,Boolean](v, w)(op) })

  def &&(that: Partitioner[K]) = applyPairWiseOperation(that) { _ && _ }
  def ||(that: Partitioner[K]) = applyPairWiseOperation(that) { _ || _ }
  def and(that: Partitioner[K]) = &&(that)
  def or(that: Partitioner[K]) = ||(that)
  def unary_! = new Partitioner[K](parent.mapValues(x => x.map(v => (v._1, !v._2))))
}


