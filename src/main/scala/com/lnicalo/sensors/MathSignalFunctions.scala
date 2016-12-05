package com.lnicalo.sensors

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
  def unary_- = new Signal[K, V](self.parent.mapValues(
    x => x.map(v => (v._1, implicitly[Fractional[V]] negate v._2))))

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
}

