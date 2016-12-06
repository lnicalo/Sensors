package com.lnicalo.sensors

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

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


  def avg(): Signal[K,V] = self.addOp(MathSignalFunctions.avg[V])

  def area(): Signal[K,V] = self.addOp(MathSignalFunctions.area[V])

  def span(): Signal[K,V] = self.addOp(MathSignalFunctions.span[V])
}

object MathSignalFunctions {
  def span[V: Fractional](x: Series[V]): HashMap[String, Option[V]] = {
    // Filter all nones out
    val tmp = x.filter({case (t, Some(x)) => true case _ => false})

    (tmp.headOption, tmp.lastOption) match {
      case (Some((_, Some(first))), Some((_, Some(last)))) =>
        HashMap[String, Option[V]]("Span" -> Option(implicitly[Fractional[V]] minus (last, first)))
      case _ => HashMap[String, Option[V]]("Span" -> None)
    }
  }

  def avg[V: Fractional](x: Series[V]): HashMap[String, Option[Double]] = {
    val (total, total_w) = x.sliding(2).map(slide => (slide.head._2, slide.last._1 - slide.head._1))
      .foldLeft((0.0, 0.0)) {
        case ((sum, acc_w), (Some(v), w)) =>
          (implicitly[Fractional[V]].toDouble(v) * w + sum, acc_w + w)
        case (acc, _) => acc
      }
    total_w match {
      case 0 => HashMap[String, Option[Double]]("Avg" -> None)
      case _ => HashMap[String, Option[Double]]("Avg" -> Some(total / total_w))
    }
  }

  def area[V: Fractional](x: Series[V]): HashMap[String, Option[Double]] = {
    val acc: Option[Double] = None
    val dbg = x.sliding(2)
      .map(slide => (slide.head._2, slide.last._1 - slide.head._1)).toList

    val total = dbg.foldLeft(acc) {
        case (None, (Some(v), w)) =>
          Some(implicitly[Fractional[V]].toDouble(v) * w)
        case (Some(current_acc), (Some(v), w)) =>
          Some(implicitly[Fractional[V]].toDouble(v) * w + current_acc)
        case (current_acc, _) =>
          current_acc
      }
    HashMap[String, Option[Double]]("Area" -> total)
  }
}
