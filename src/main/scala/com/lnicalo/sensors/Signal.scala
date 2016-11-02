/**
  * Created by LNICOLAS on 31/10/2016.
  */
package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Random

class Signal[K, V: Fractional](val parent: RDD[(K, List[(Double, V)])])
               (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, List[(Double, V)])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, V)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def applyPairWiseOperation(that: Signal[K, V])(op: (V, V) => V): Signal[K, V] =
    new Signal[K, V](this.join(that).mapValues { case (v, w) => Signal.PairWiseOperation[V, V](v, w)(op) })

  def applyBooleanOperation(that: Signal[K, V])(op: (V, V) => Boolean): Partitioner[K] = {
    new Partitioner[K](this.join(that).mapValues { case (v, w) => Signal.PairWiseOperation[V, Boolean](v, w)(op) })
  }

  def applyPairWiseOperation(that: V)(op: (V, V) => V): Signal[K, V] =
    new Signal[K, V](this.mapValues(x => x.map(v => (v._1, op(v._2, that)))))

  def applyBooleanOperation(that: V)(op: (V, V) => Boolean): Partitioner[K] =
    new Partitioner[K](this.mapValues(x => x.map(v => (v._1, op(v._2, that)))))

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Math operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  // Sum
  def +(that: Signal[K, V]) = applyPairWiseOperation(that) { implicitly[Fractional [V]] plus (_, _) }
  def +(that: V) = applyPairWiseOperation(that) { implicitly[Fractional [V]] plus (_, _) }
  def +:(that: V) = this + that

  // Minus
  def -(that: Signal[K, V]) = applyPairWiseOperation(that) { implicitly[Fractional [V]] minus (_, _) }
  def -(that: V) = applyPairWiseOperation(that) { implicitly[Fractional [V]] minus (_, _) }
  def -:(that: V) = applyPairWiseOperation(that) { (x, y) => implicitly[Fractional [V]] minus (y, x) }

  // Negate
  def unary_- = new Signal[K, V](parent.mapValues(x => x.map(v => (v._1, implicitly[Fractional [V]] negate v._2))))

  // Division
  def /(that: Signal[K, V]) = applyPairWiseOperation(that) { implicitly[Fractional [V]] div (_, _) }
  def /(that: V) = applyPairWiseOperation(that) { implicitly[Fractional [V]] div (_, _) }
  def /:(that: V) = applyPairWiseOperation(that) { (x, y) => implicitly[Fractional [V]] div (y, x) }

  // Multiplication
  def *(that: Signal[K, V]) = applyPairWiseOperation(that) { implicitly[Fractional [V]] times (_, _) }
  def *(that: V) = applyPairWiseOperation(that) { implicitly[Fractional [V]] times (_, _) }
  def *:(that: V) = this * that

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Boolean operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  def >(that: V) = applyBooleanOperation(that) { implicitly[Fractional [V]] gt (_, _) }
  def >=(that: V) = applyBooleanOperation(that) { implicitly[Fractional [V]] gteq (_, _) }
  def <(that: V) = applyBooleanOperation(that) { implicitly[Fractional [V]] lt (_, _) }
  def <=(that: V) = applyBooleanOperation(that) { implicitly[Fractional [V]] lteq (_, _) }
  def ==(that: V) = applyBooleanOperation(that) { implicitly[Fractional [V]] equiv (_, _) }

  def >(that: Signal[K, V]) = applyBooleanOperation(that) { implicitly[Fractional [V]] gt (_, _) }
  def >=(that: Signal[K, V]) = applyBooleanOperation(that) { implicitly[Fractional [V]] gteq (_, _) }
  def <(that: Signal[K, V]) = applyBooleanOperation(that) { implicitly[Fractional [V]] lt (_, _) }
  def <=(that: Signal[K, V]) = applyBooleanOperation(that) { implicitly[Fractional [V]] lteq (_, _) }
  def ==(that: Signal[K, V]) = applyBooleanOperation(that) { implicitly[Fractional [V]] equiv (_, _) }


}

object Signal {
  def PairWiseOperation[V,O](v: List[(Double, V)], w: List[(Double, V)])(f: (V,V) => O): List[(Double, O)] = {
    @tailrec
    def recursive(v: List[(Double, V)], w: List[(Double, V)], acc: List[(Double, O)]): List[(Double, O)] = {
      if (v.isEmpty || w.isEmpty) acc
      else {
        val d = w.head
        val e = v.head
        if (d._1 == e._1) {
          val s = (d._1, f(e._2, d._2))
          recursive(v.tail, w.tail, s :: acc)
        }
        else if (d._1 > e._1) {
          val s = (d._1, f(e._2, d._2))
          recursive(v, w.tail, s :: acc)
        } else {
          val s = (e._1, f(e._2, d._2))
          recursive(v.tail, w, s :: acc)
        }
      }
    }
    recursive(v.reverse, w.reverse, Nil)
  }

  def uniform_random_sample (n_samples: Array[Int]) (implicit sc: SparkContext): Signal[String, Double] = {
    var index = 0
    val data = n_samples.map(n => {
      index += 1
      (index.toString, (0 until n).map(x => (x.toDouble, Random.nextDouble)).toList)
    })
    new Signal(sc.parallelize(data, 1))
  }

  def apply[K, V: Fractional](local: Array[(K, List[(Double, V)])])
              (implicit sc: SparkContext, kClassTag: ClassTag[K]): Signal[K, V] =
    new Signal[K, V](sc.parallelize(local))
}

