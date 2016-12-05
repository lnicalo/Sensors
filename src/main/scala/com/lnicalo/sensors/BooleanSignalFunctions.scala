package com.lnicalo.sensors

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BooleanSignalFunctions[K: ClassTag](self: Signal[K, Boolean])
  extends Serializable {

  ///////////////////////////////////////////////////////////////////////////////////////
  //
  // Boolean operations
  //
  ///////////////////////////////////////////////////////////////////////////////////////
  def &&(that: Signal[K, Boolean]): Signal[K, Boolean] =
    self.applyPairWiseOperation(that) { _ && _ }
  def ||(that: Signal[K, Boolean]): Signal[K, Boolean] =
    self.applyPairWiseOperation(that) { _ || _ }
  def and(that: Signal[K, Boolean]): Signal[K, Boolean] = &&(that)
  def or(that: Signal[K, Boolean]): Signal[K, Boolean] = ||(that)
  def unary_! : Signal[K, Boolean] = new Signal[K, Boolean](
    self.mapValues(x =>
      x.map { v =>
        v match {
          case (t, Some(x)) => (t, Option(!x))
          case (t, None) => (t, None)
        }
      }
    )
  )
}



