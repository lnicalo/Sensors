package com.lnicalo.sensors
import scala.language.implicitConversions

import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class FilteredSignal[K: ClassTag, V: ClassTag](val parent: RDD[(K, List[List[(Double, V)]])])
  extends RDD[(K, List[List[(Double, V)]])](parent) {

  def compute(split: Partition, context: TaskContext): Iterator[(K, List[List[(Double, V)]])] =
    parent.iterator(split, context)

  protected def getPartitions: Array[Partition] = parent.partitions

  def flatten() = {
    val rdd = parent.flatMap { case (key, value) =>
      for (s <- value) yield ((key, (s.head._1, s.last._1)), s)
    }
    Signal[(K, (Double, Double)), V](rdd)
  }
}

object FilteredSignal {
  implicit def signalToStringSignalFunctions[K] (filteredSignal: FilteredSignal[K, String])
                                                (implicit kt: ClassTag[K]):
    StringSignalFunctions[(K, (Double, Double))] = {
      new StringSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToMathSignalFunctions[K, V: Fractional] (filteredSignal: FilteredSignal[K, V])
                                                             (implicit kt: ClassTag[K], vt: ClassTag[V]):
     MathSignalFunctions[(K, (Double, Double)), V] = {
      new MathSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToBooleanSignalFunctions[K] (filteredSignal: FilteredSignal[K, Boolean])
                                                (implicit kt: ClassTag[K]):
  BooleanSignalFunctions[(K, (Double, Double))] = {
    new BooleanSignalFunctions(filteredSignal.flatten())
  }

  implicit def signalToSignal[K, V] (filteredSignal: FilteredSignal[K, V])
                                    (implicit kt: ClassTag[K], vt: ClassTag[V]):
  Signal[(K, (Double, Double)), V] = {
    filteredSignal.flatten()
  }
}

