package com.lnicalo.sensors

import org.apache.spark.rdd._
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by LNICOLAS on 04/11/2016.
  */
class GetTimings[K: ClassTag, M: ClassTag] (parent: RDD[(K, HashMap[String, M])])
  extends Operation(parent)

object GetTimings {
  def apply[K: ClassTag,V: ClassTag](signal: Signal[K,V]) = {
    new GetTimings(signal.timings())
  }
}