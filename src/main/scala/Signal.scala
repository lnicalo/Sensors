/**
  * Created by LNICOLAS on 31/10/2016.
  */
package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import TimeSeriesUtils.PairWiseOperation

import scala.reflect.ClassTag
import scala.util.Random

class Signal[K](val parent: RDD[(K, List[(Double, Double)])])
               (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, List[(Double, Double)])](parent){

  def apply_operation(that: RDD[(K, List[(Double, Double)])]) (op: (Double, Double) => Double):
    Signal[K] =
      new Signal[K](parent.join(that).mapValues { case (v, w) => PairWiseOperation[Double](v, w)(op) })

  def apply_operation(that: Double) (op: (Double, Double) => Double):
    Signal[K] =
      new Signal[K](parent.mapValues(x => x.map(v => (v._1, op(v._2, that)))))

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, Double)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def +(that: Signal[K]) = apply_operation(that) {_ + _}
  def +(that: Double) = apply_operation(that) {_ + _}

  def -(that: Signal[K]) = apply_operation(that) {_ - _}
  def -(that: Double) = apply_operation(that) {_ - _}

  def /(that: Signal[K]) = apply_operation(that) {_ / _}
  def /(that: Double) = apply_operation(that) {_ / _}

  def *(that: Signal[K]) = apply_operation(that) {_ * _}
  def *(that: Double) = apply_operation(that) {_ * _}
}

object Signal {
  def uniform_random_sample (n_samples: Array[Int]) (implicit sc: SparkContext):
    Signal[String] = {
    var index = 0
    val data = n_samples.map(n => {
      index += 1
      (index.toString, (0 until n).map(x => (x.toDouble, Random.nextDouble)).toList)
    })
    new Signal(sc.parallelize(data, 1))
  }

  def apply[K](local: Array[(K, List[(Double, Double)])])
              (implicit sc: SparkContext, kClassTag: ClassTag[K]): Signal[K] =
    new Signal(sc.parallelize(local))
}

