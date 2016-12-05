package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.util.Random

class Signal[K: ClassTag, V : ClassTag](val parent: RDD[(K, Series[V])])
  extends RDD[(K, Series[V])](parent) {

  type Ops = (Series[V]) => HashMap[String, Any]
  var ops = List[Ops]()

  def compute(split: Partition, context: TaskContext): Iterator[(K, Series[V])] = {
    parent.iterator(split, context)
  }

  protected def getPartitions: Array[Partition] = parent.partitions

  private[sensors] def applyPairWiseOperation[O: ClassTag](that: Signal[K, V])
                                                          (op: (V, V) => O):
  Signal[K, O] = {
    new Signal(parent.join(that.parent).mapValues {
      case (v, w) => Signal.PairWiseOperation[V, V, O](v, w) {
        (a,b) => (a,b) match {
          case (Some(x), Some(y)) => Option(op(x, y))
          case _ => None
        }
      }
    })
  }

  private[sensors] def applyPairWiseOperation[O: ClassTag](that: V)
                                                          (op: (V, V) => O):
  Signal[K, O] = {
    new Signal(parent.mapValues {x =>
      x.map { v =>
        v match {
          case (t, Some(v)) => (t, Some(op(v, that)))
          case (t, _) => (t, None)
        }
      }
    })
  }

  private[sensors] def applyPairWiseOperationX[O: ClassTag](that: Signal[K, V])
                                                          (op: (Value[V], Value[V]) => Value[O]):
  Signal[K, O] = {
    new Signal(parent.join(that.parent).mapValues {
      case (v, w) => Signal.PairWiseOperation[V, V, O](v, w)(op)
    })
  }

  private[sensors] def applyPairWiseOperationV[O: ClassTag](that: V)
                                                          (op: (Value[V], Value[V]) => Value[O]):
  Signal[K, O] = {
    new Signal(parent.mapValues {
      x => x.map(v => (v._1, op(v._2, Option(that))))
    })

  }

  // Filter
  def where(that: Signal[K, Boolean]): Signal[K, V] = {
    new Signal(parent.join(that).mapValues {
      case (v, filter) => Signal.PairWiseFilter(v, filter)
    })
  }

  // Operations
  def duration() = this.addOp(Signal.duration)

  def lastValue(): Signal[K,V] = this.addOp(Signal.last)

  def firstValue(): Signal[K,V] = this.addOp(Signal.first)

  def addOp(f: Ops): Signal[K, V] = {
    this.ops ::= f
    this
  }

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

object Signal {

  implicit def signalToBooleanSignalFunctions[K]
   (signal: Signal[K, Boolean])
   (implicit kt: ClassTag[K]):
   BooleanSignalFunctions[K] = {
      new BooleanSignalFunctions[K](signal)
  }

  implicit def signalToMathSignalFunctions[K, V: Fractional]
    (signal: Signal[K, V])
    (implicit kt: ClassTag[K], vt: ClassTag[V]):
    MathSignalFunctions[K,V] = {
      new MathSignalFunctions(signal)
    }

  implicit def signalToStringSignalFunctions[K]
    (signal: Signal[K, String])
    (implicit kt: ClassTag[K]):
    StringSignalFunctions[K] = {
      new StringSignalFunctions(signal)
    }

  def start[V](x: Series[V]): HashMap[String, Option[Double]] = {
    val tmp = x.filter({ case (_, Some(_)) => true case _ => false })
    tmp.headOption match {
      case Some((startTime, _)) => HashMap[String, Option[Double]]("Start" -> Some(startTime))
      case _ => HashMap[String, Option[Double]]("Start" -> None)
    }
  }

  def end[V](x: Series[V]): HashMap[String, Option[Double]] = {
    val tmp = x.sliding(2).map(h => (h.last._1, h.head._2))
      .filter({ case (_, Some(x)) => true case _ => false })
    tmp.toList.lastOption match {
      case Some((endTime, _)) => HashMap[String, Option[Double]]("End" -> Some(endTime))
      case _ => HashMap[String, Option[Double]]("End" -> None)
    }
  }

  def duration[V](x: Series[V]): HashMap[String, Option[Double]] = {
    val duration = x.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1))
      .filter({ case (Some(_), _) => true case _ => false })
      .foldLeft(0.0) { (a, b) => a + b._2 }
    HashMap[String, Some[Double]]("Duration" -> Some(duration))
  }

  def last[V](x: Series[V]): HashMap[String, Option[V]] = {
    val tmp = x.filter({case (t, Some(x)) => true case _ => false})
    (tmp.lastOption) match {
      case Some((_, lastValue)) => HashMap("Last" -> lastValue)
      case _ => HashMap("Last" -> None)
    }
  }

  def first[V](x: Series[V]): HashMap[String, Option[V]] =  {
    (x.headOption) match {
      case Some((_, firstValue)) => HashMap("First" -> firstValue)
      case _ => HashMap("First" -> None)
    }
  }



  def PairWiseOperation[A,B,O](v: Series[A],
                               w: Series[B])
                              (f: (Value[A],Value[B]) => Value[O]):
  Series[O] = {
    @tailrec
    def recursive(v: Series[A],
                  w: Series[B],
                  acc: Series[O]): Series[O] = {
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

  def PairWiseFilter[A](v: Series[A], filter: Series[Boolean]): Series[A] =
    Signal.PairWiseOperation(v, filter)((a,b) => if (b.getOrElse(false)) a else None)

  def uniform_random_sample (n_samples: Array[Int]) (implicit sc: SparkContext) = {
    var index = 0
    val data = n_samples.map(n => {
      index += 1
      (index.toString, (0 until n).map(x => (x.toDouble, Option(Random.nextDouble))).toList)
    })
    new Signal(sc.parallelize(data, 1))
  }

  def apply[K: ClassTag, V: ClassTag](local: Array[(K, List[(Double, V)])])
                                                 (implicit sc: SparkContext) =
    new Signal[K, V](sc.parallelize(local.map {
      case (a, b) => (a, b.map {
        case (t, v) => (t, Option(v))
      })
    } ))

  def apply[K: ClassTag, V: ClassTag](rdd: RDD[(K, Series[V])]) =
    new Signal[K,V](rdd)
}

