package com.lnicalo.sensors

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 01/12/2016.
  */
class MathSignalFunctions[K: ClassTag, V : Fractional: ClassTag] (self: Signal[K, V])
  extends Serializable {

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Math operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  // Sum
  def |+|(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] plus(_, _)
  }

  def |+|(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] plus(_, _)
  }

  def +:(that: V) = this |+| that

  // Minus
  def -(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] minus(_, _)
  }

  def -(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] minus(_, _)
  }

  def -:(that: V) = self.applyPairWiseOperation(that) {
    (x, y) => implicitly[Fractional[V]] minus(y, x)
  }

  // Negate
  def unary_- = new Signal[K, V](self.parent.mapValues { x =>
    x.map {v =>
      v match {
        case (t, Some(x)) => (t, Some(implicitly[Fractional[V]] negate x))
        case (t, None) => (t, None)
      }
    }
  })

  // Division
  def /(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] div(_, _)
  }

  def /(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] div(_, _)
  }

  def /:(that: V) = self.applyPairWiseOperation(that) {
    (x, y) => implicitly[Fractional[V]] div(y, x)
  }

  // Multiplication
  def *(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] times(_, _)
  }

  def *(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] times(_, _)
  }

  def *:(that: V) = this * that

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Boolean operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  def >(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] gt(_, _)
  }

  def >=(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] gteq(_, _)
  }

  def <(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] lt(_, _)
  }

  def <=(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] lteq(_, _)
  }

  def |==|(that: V) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] equiv(_, _)
  }

  def >(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] gt(_, _)
  }

  def >=(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] gteq(_, _)
  }

  def <(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] lt(_, _)
  }

  def <=(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] lteq(_, _)
  }

  def |==|(that: Signal[K, V]) = self.applyPairWiseOperation(that) {
    implicitly[Fractional[V]] equiv(_, _)
  }


  def avg(): Signal[K,V] = self // .addOp(MathSignalFunctions.avg[V])

  def area(): Signal[K,V] = self // .addOp(MathSignalFunctions.area[V])

  def span(): Signal[K,V] = self // .addOp(MathSignalFunctions.span[V])
}

object MathSignalFunctions {
  def span[V: Fractional](x: List[(Double, V)]): HashMap[String, V] = {
    val a: HashMap[String, V] = Signal.last(x)
    val b: HashMap[String, V] = Signal.first(x)
    var out = b
    out = out.updated("Last", a("Last"))
    out.updated("Span", implicitly[Fractional[V]].minus(a("Last"), b("First")))
  }

  def avg[V: Fractional](x: List[(Double, V)]): HashMap[String, Double] = {
    val y = x.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1)).toList
    val avg = y.foldLeft(0.0) { (a, b) => a + implicitly[Fractional[V]].toDouble(b._1) * b._2 } /
      y.foldLeft(0.0) { (a, b) => a + b._2 }
    HashMap[String, Double]("Avg" -> avg)
  }

  def area[V: Fractional](x: List[(Double, V)]): HashMap[String, Double] = {
    val y = x.sliding(2).map(h => (h.head._2, h.last._1 - h.head._1)).toList
    val area = y.foldLeft(0.0) { (a, b) => a + implicitly[Fractional[V]].toDouble(b._1) * b._2 }
    HashMap[String, Double]("Area" -> area)
  }
}
