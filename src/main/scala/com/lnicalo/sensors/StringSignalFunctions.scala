package com.lnicalo.sensors

import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 01/12/2016.
  */
class StringSignalFunctions[K: ClassTag] (self: Signal[K, String])
  extends Serializable {

  def |==|(that: String): Signal[K, Boolean] = self.applyBooleanOperation(that) {
    _ == _
  }

  def |==|(that: Signal[K, String]): Signal[K, Boolean] = self.applyBooleanOperation(that) {
    _ == _
  }
}
