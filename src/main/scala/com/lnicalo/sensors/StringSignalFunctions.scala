package com.lnicalo.sensors

import scala.reflect.ClassTag

class StringSignalFunctions[K: ClassTag] (self: Signal[K, String])
  extends Serializable {

  def |==|(that: String): Signal[K, Boolean] = self.applyPairWiseOperation(that) {
    _ == _
  }

  def |==|(that: Signal[K, String]): Signal[K, Boolean] = self.applyPairWiseOperation(that) {
    _ == _
  }
}
