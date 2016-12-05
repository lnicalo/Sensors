package com.lnicalo.sensors

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 01/12/2016.
  */
class MathFilteredSignalFunctions[K: ClassTag, V : Fractional: ClassTag] (self: FilteredSignal[K, V])
  extends Serializable {

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Summary operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////

  def avg(): FilteredSignal[K,V] = self.addOp(MathFilteredSignalFunctions.avg[V])

  def area(): FilteredSignal[K,V] = self.addOp(MathFilteredSignalFunctions.area[V])

  def span(): FilteredSignal[K,V] = self.addOp(MathFilteredSignalFunctions.span[V])
}

object MathFilteredSignalFunctions {
  implicit def signalToMathSignalFunctions[K, V: Fractional]
  (signal: FilteredSignal[K, V])
  (implicit kt: ClassTag[K], vt: ClassTag[V]):
  MathFilteredSignalFunctions[K,V] = {
    new MathFilteredSignalFunctions(signal)
  }

  def span[V: Fractional](x: List[List[(Double, V)]]): HashMap[String, V] = {
    val tmp = x.last
    val last =  if (tmp.length > 1) tmp(tmp.length - 2)._2 else tmp.last._2
    val first = x.head.head._2
    HashMap[String, V]("First" -> first,
                       "Last" -> last,
                       "Span" -> implicitly[Fractional[V]].minus(last, first))
  }

  def avg[V: Fractional](x: List[List[(Double, V)]]):
  HashMap[String, Double] = {
    val (areaValue, weightSum) = x.foldLeft((0.0, 0.0)) {
      (acc, subList) =>
        // Iterator with slices
        val y = subList.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1)).toList
        // Compute sub areas and sum
        val subArea = y.foldLeft(0.0) {
          (a, b) => a + implicitly[Fractional[V]].toDouble(b._1) * b._2
        }
        val subWeightSum = y.foldLeft(0.0) {
          (a, b) => a + b._2
        }
        (acc._1 + subArea, acc._2 + subWeightSum)
    }
    HashMap[String, Double]("Avg" -> areaValue / weightSum)
  }

  def area[V: Fractional](x: List[List[(Double, V)]]): HashMap[String, Double] = {
    val areaValue = x.foldLeft(0.0) {
      (acc, subList) =>
        // Iterator with slices
        val y = subList.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1)).toList
        // Compute sub areas and sum
        val subArea = y.foldLeft(0.0) {
          (a, b) => a + implicitly[Fractional[V]].toDouble(b._1) * b._2
        }
        acc + subArea
    }
    HashMap[String, Double]("Area" -> areaValue)
  }
}

