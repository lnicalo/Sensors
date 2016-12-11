package com.lnicalo.sensors

import org.apache.spark.rdd._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.mutable.{ListBuffer, Map}
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
      case (v, filter) =>
        Signal.PairWiseFilter(v, filter)
    })
  }

  // Bin
  def binBy[W](that: Signal[K, W]): Signal[(K, W), V] = {
    val o = parent.join(that).flatMap ({
      case (key, (v, filter)) =>
        Signal.BinOperation(v, filter).map({ case (k, ss) => ((key, k), ss)})
    })
    new Signal(o)
  }

  def splitBy[W](that: Signal[K, W]): Signal[(K, (Double, Double)), V] = {
    val o = parent.join(that).flatMap ({
      case (key, (v, filter)) =>
        Signal.SplitOperation(v, filter).map({ case (k, ss) => ((key, k), ss.toList)})
    })
    new Signal(o)
  }

  // Operations
  def start() = this.addOp(Signal.start)

  def end() = this.addOp(Signal.end)

  def duration() = this.addOp(Signal.duration)

  def lastValue(): Signal[K,V] = this.addOp(Signal.lastValue)

  def firstValue(): Signal[K,V] = this.addOp(Signal.firstValue)

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
      case Some((startTime, _)) =>
        HashMap[String, Option[Double]]("Start" -> Some(startTime))
      case _ =>
        HashMap[String, Option[Double]]("Start" -> None)
    }
  }

  def end[V](x: Series[V]): HashMap[String, Option[Double]] = {
    val tmp = x.sliding(2).map(h => (h.last._1, h.head._2))
      .filter({ case (_, Some(x)) => true case _ => false })
    tmp.toList.lastOption match {
      case Some((endTime, _)) =>
        HashMap[String, Option[Double]]("End" -> Some(endTime))
      case _ =>
        HashMap[String, Option[Double]]("End" -> None)
    }
  }

  def duration[V](x: Series[V]): HashMap[String, Option[Double]] = {
    val duration = x.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1))
      .filter({ case (Some(_), _) => true case _ => false })
      .foldLeft(0.0) { (a, b) => a + b._2 }
    HashMap[String, Some[Double]]("Duration" -> Some(duration))
  }

  def lastValue[V](x: Series[V]): HashMap[String, Option[V]] = {
    val tmp = x.filter({case (t, Some(x)) => true case _ => false})
    (tmp.lastOption) match {
      case Some((_, value)) => HashMap("Last" -> value)
      case _ => HashMap("Last" -> None)
    }
  }

  def firstValue[V](x: Series[V]): HashMap[String, Option[V]] =  {
    (x.find({case (_, Some(_)) => true case _ => false})) match {
      case Some((_, value)) => HashMap("First" -> value)
      case _ => HashMap("First" -> None)
    }
  }

  def SplitOperation[A,B](series: Series[A], splitter: Series[B]):
  List[((Double, Double), SeriesBuffer[A])] = {
    def splitBySeparator[T]( l: Series[T]): ListBuffer[SeriesBuffer[T]] = {
      val b = ListBuffer(ListBuffer[(Double, Option[T])]())
      l foreach {
        sample => sample match {
          case (time, Some(value)) => b.last += sample
          case _ => if  ( !b.last.isEmpty ) {
            b.last += sample
            b += ListBuffer[(Double, Option[T])]()
          }
        }
      }
      b
    }

    val tmp = BinOperation[A,B](series, splitter)
    val b = ListBuffer(ListBuffer[(Double, Option[A])]())
    for ((key, key_series) <- tmp) {
      b ++= splitBySeparator(key_series)
    }
    b.filter(_.length > 0)
      .map(series => ((series.head._1, series.last._1), series))
      .toList
  }

  def SplitOperation2[A,B](series: Series[A], splitter: Series[B]):
    List[(B, SeriesBuffer[A])] = {
    def appendToSeries[V](acc: SeriesBuffer[V], samples: (Double, Value[V])*): SeriesBuffer[V] = {
      var out = acc
      for (s <- samples) {
        if (acc.length < 1 || s._2 != acc.head._2) out += s
        else out(out.length-1) = s
      }
      out
    }

    @tailrec
    def recursive(series: Series[A], splitter: Series[B],
                  acc: ListBuffer[(B, SeriesBuffer[A])]): ListBuffer[(B, SeriesBuffer[A])] = {

      (series, splitter) match {

        case ((sample_t, sample_v) :: series_tail, (split_t, Some(split_v)) :: split_tail) =>
          if (sample_t <= split_t) {
            val tmp = appendToSeries(
              acc.headOption.getOrElse((split_v, ListBuffer[(Double, Value[A])]((split_t, None))))._2,
              (split_t, sample_v))
            split_tail.headOption match {
              case Some((split_t2, Some(split_v2))) if (split_v2 != split_v) =>
                val tmp2 = appendToSeries(
                  acc.headOption.getOrElse((split_v, ListBuffer[(Double, Value[A])]((split_t, None))))._2,
                  (split_t, None), (split_t, sample_v))
                acc.append((split_v,tmp), (split_v2,tmp2))
              case _ =>
                acc.append((split_v, tmp))
            }
            if (sample_t == split_t) recursive(series_tail, split_tail, acc)
            else recursive(series, split_tail, acc)
          }
          else {
            val tmp = appendToSeries(
              acc.headOption.getOrElse(split_v, ListBuffer[(Double, Value[A])]((sample_t, None)))._2,
              (sample_t, sample_v))
            recursive(series_tail, splitter, acc +=(split_v -> tmp))
          }

        case ((sample_t, _) :: series_tail, (split_t, _) :: split_tail) =>
          if (sample_t > split_t) recursive(series_tail, splitter, acc)
          else recursive(series, split_tail, acc)

        case _ => acc
      }
    }
    recursive(series.reverse, splitter.reverse, ListBuffer[(B, SeriesBuffer[A])]()).toList
  }

  def BinOperation[A,B](v: Series[A], splitter: Series[B]): Map[B, Series[A]] = {
    def appendToSeries[V](acc: Series[V], samples: (Double, Value[V])*): Series[V] = {
      var out = acc
      for (s <- samples) {
        out = if (acc.length < 1 || s._2 != acc.head._2) s :: out
        else s :: out.tail
      }
      out
    }

    @tailrec
    def recursive(series: Series[A], splitter: Series[B],
                  acc: Map[B, Series[A]]): Map[B, Series[A]] = {

      (series, splitter) match {

        case ((sample_t, sample_v) :: series_tail, (split_t, Some(split_v)) :: split_tail) =>
          if (sample_t <= split_t) {
            val tmp = appendToSeries(
              acc.getOrElse(split_v, List((split_t, None))), (split_t, sample_v))

            split_tail.headOption match {
              case Some((split_t2, Some(split_v2))) if (split_v2 != split_v) =>
                val tmp2 = appendToSeries(
                  acc.getOrElse(split_v2, List((split_t, None))),
                  (split_t, None), (split_t, sample_v))
                acc += ((split_v -> tmp), (split_v2 -> tmp2))
              case _ =>
                acc += (split_v -> tmp)
            }
            if (sample_t == split_t) recursive(series_tail, split_tail, acc)
            else recursive(series, split_tail, acc)
          }
          else {
            val tmp = appendToSeries(
              acc.getOrElse(split_v, List((sample_t, None))),
              (sample_t, sample_v))
            recursive(series_tail, splitter, acc +=(split_v -> tmp))
          }

        case ((sample_t, _) :: series_tail, (split_t, _) :: split_tail) =>
          if (sample_t > split_t) recursive(series_tail, splitter, acc)
          else recursive(series, split_tail, acc)

        case _ => acc
      }
    }
    recursive(v.reverse, splitter.reverse, Map[B, Series[A]]())
  }

  def PairWiseOperation[A,B,O](v: Series[A], w: Series[B])
                              (f: (Value[A],Value[B]) => Value[O]):
  Series[O] = {
    def updateAcc[V](acc: Series[V], s: (Double, Value[V])) = {
      if (acc.length < 2) {
        s :: acc
      } else {
        val old_value = acc.head._2
        if (s._2 != old_value) {
          s :: acc
        }
        else {
          s :: acc.tail
        }
      }
    }

    @tailrec
    def recursive(v: Series[A],
                  w: Series[B],
                  acc: Series[O]): Series[O] = {
      if (v.isEmpty || w.isEmpty) acc
      else {
        val d = w.head
        val e = v.head
        val new_val = f(e._2, d._2)
        if (d._1 == e._1) {
          val s = (d._1, new_val)
          recursive(v.tail, w.tail, updateAcc(acc, s))
        }
        else if (d._1 > e._1) {
          val s = (d._1, new_val)
          recursive(v, w.tail, updateAcc(acc, s))
        } else {
          val s = (e._1, new_val)
          recursive(v.tail, w, updateAcc(acc, s))
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
      case (a, b) => (a, toSeries(b))
      }))

  def apply[K: ClassTag, V: ClassTag](rdd: RDD[(K, Series[V])]) =
    new Signal[K,V](rdd)
}

