package com.lnicalo.sensors
import scala.language.implicitConversions
import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class Signal[K: ClassTag, V : ClassTag](val parent: RDD[(K, List[(Double, V)])])
  extends RDD[(K, List[(Double, V)])](parent) {

  type Ops = (List[(Double, V)]) => HashMap[String, Any]
  var ops = List[Ops]()

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[(Double, V)])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  private[sensors] def applyPairWiseOperation[O: ClassTag](that: Signal[K, V])(op: (V, V) => O): Signal[K, O] =
    new Signal(parent.join(that.parent).mapValues {
      case (v, w) => Signal.PairWiseOperation[V, V, O](v, w)(op)
    })

  private[sensors] def applyPairWiseOperation[O: ClassTag](that: V)(op: (V, V) => O): Signal[K, O] =
    new Signal(parent.mapValues {
      x => x.map(v => (v._1, op(v._2, that)))
    })

  // Filter
  def where(that: Signal[K, Boolean]): FilteredSignal[K, V] = {
    new FilteredSignal(parent.join(that).mapValues {
      case (v, filter) => Signal.FilterOperation(v, filter)
    })
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
  implicit def signalToFilteredSignal[K, V]
  (signal: Signal[K, V])
  (implicit kt: ClassTag[K], vt: ClassTag[V]):
  FilteredSignal[K, V] = {
    new FilteredSignal[K, V](signal.mapValues(x => List(x)))
  }

  implicit def mathSignalToMathFilteredSignalFunctions[K, V: Fractional]
  (signal: Signal[K, V])
  (implicit kt: ClassTag[K], vt: ClassTag[V], f: Signal[K,V] => FilteredSignal[K,V]):
  MathFilteredSignalFunctions[K, V] = {
    new MathFilteredSignalFunctions[K, V](signal)
  }

  implicit def signalToBooleanSignalFunctions[K, V: Fractional]
  (signal: FilteredSignal[K, V])
  (implicit kt: ClassTag[K], vt: ClassTag[V]):
  MathFilteredSignalFunctions[K, V] = {
    new MathFilteredSignalFunctions[K, V](signal)
  }

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

  def uniform_random_sample (n_samples: Array[Int]) (implicit sc: SparkContext) = {
    var index = 0
    val data = n_samples.map(n => {
      index += 1
      (index.toString, (0 until n).map(x => (x.toDouble, Random.nextDouble)).toList)
    })
    new Signal(sc.parallelize(data, 1))
  }

  def apply[K: ClassTag, V: ClassTag](local: Array[(K, List[(Double, V)])])
                                                 (implicit sc: SparkContext) =
    new Signal(sc.parallelize(local))

  def apply[K: ClassTag, V: ClassTag](rdd: RDD[(K, List[(Double, V)])]) =
    new Signal(rdd)

  /*def apply[K: ClassTag, V: ClassTag](): Signal[K,V] = {
    val tableName = ""

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName )
    val sc = new SparkContext(sparkConf)

    val rdd = sc.hbaseTable[Double]("table")
      .select("col")
      .inColumnFamily("columnFamily")
      .withStartRow("00501")
      .withSalting((0 to 9).map(s => s.toString))

    rdd.
    new Signal(rdd)
  }*/
}

