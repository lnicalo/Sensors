package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class Signal[K, V: Fractional](val parent: RDD[(K, List[(Double, V)])])
                              (implicit kt: ClassTag[K], vt: ClassTag[V])
  extends RDD[(K, List[(Double, V)])](parent){

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, V)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  private def applyPairWiseOperation(that: Signal[K, V])(op: (V, V) => V): Signal[K, V] =
    new Signal[K, V](this.join(that).mapValues { case (v, w) => Signal.PairWiseOperation[V,V,V](v, w)(op) })

  private def applyBooleanOperation(that: Signal[K, V])(op: (V, V) => Boolean): Filter[K] =
    new Filter[K](this.join(that).mapValues { case (v, w) => Signal.PairWiseOperation[V,V,Boolean](v, w)(op) })

  private def applyPairWiseOperation(that: V)(op: (V, V) => V): Signal[K, V] =
    new Signal[K, V](this.mapValues(x => x.map(v => (v._1, op(v._2, that)))))

  private def applyBooleanOperation(that: V)(op: (V, V) => Boolean): Filter[K] =
    new Filter[K](this.mapValues(x => x.map(v => (v._1, op(v._2, that)))))

  private def applyFilter(that: Filter[K]): FilteredSignal[K, V] =
    new FilteredSignal[K, V](this.join(that).mapValues { case (v, filter) => Signal.FilterOperation(v, filter) })

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

  // Filter
  def where(that: Filter[K]): FilteredSignal[K, V] = {
    applyFilter(that)
  }
}

object Signal {
  def PairWiseOperation[A,B,O](v: List[(Double, A)], w: List[(Double, B)])(f: (A,B) => O): List[(Double, O)] = {
    @tailrec
    def recursive(v: List[(Double, A)], w: List[(Double, B)], acc: List[(Double, O)]): List[(Double, O)] = {
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

  def FilterOperation[V](v: List[(Double, V)], filter: List[(Double, Boolean)]): List[List[(Double, V)]] = {
    @tailrec
    def recursive(v: List[(Double, V)], filter: List[(Double, Boolean)],
                  acc: List[ArrayBuffer[(Double, V)]]): List[ArrayBuffer[(Double, V)]] = {
      if (v.isEmpty || filter.isEmpty) acc
      else {
        val d = filter.head
        val e = v.head
        if (d._1 == e._1) {
          val s = if (d._2){
            acc.head.append(e)
            acc
          } else ArrayBuffer[(Double, V)]((d._1, e._2)) :: acc
          recursive(v.tail, filter.tail, s)
        }
        else if (d._1 > e._1) {
          val s = if (d._2){
            acc.head.append((d._1, e._2))
            acc
          } else  ArrayBuffer[(Double, V)]((d._1, e._2)) :: acc
          recursive(v, filter.tail, s)
        } else {
          val s: List[ArrayBuffer[(Double, V)]] = {
            if (d._2) acc.head.append(e)
            acc
          }
          recursive(v.tail, filter, s)
        }
      }
    }

    // Remove contiguous duplicate values from filter:
    //  The algorithm relies on the fact that
    //  there are not contiguous duplicates in the list that works as a filter
    // IMPORTANT NOTE: It reverses the list. Timestamps are in decreasing order
    def removeDuplicates(v: List[(Double, Boolean)]): List[(Double, Boolean)] = {
      v.foldLeft(List(v.head))((result, s) => if (s._2 == result.head._2) result else s :: result)
    }
    val tmp = removeDuplicates(filter)
    val init_acc = ArrayBuffer[(Double, V)]()
    recursive(v.reverse, tmp, List(init_acc))
      .filter(_.length > 1).map(_.toList.reverse)
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
              (implicit sc: SparkContext, kt: ClassTag[K], vt: ClassTag[V]): Signal[K, V] =
    new Signal[K, V](sc.parallelize(local))
}

