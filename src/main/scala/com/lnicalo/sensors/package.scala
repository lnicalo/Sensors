package com.lnicalo

import scala.collection.mutable.ListBuffer

package object sensors {
  type Value[V] = Option[V]
  type Series[V] = List[(Double, Value[V])]
  type SeriesBuffer[V] = ListBuffer[(Double, Value[V])]

  /* Implicit conversion to facilitate testing */
  implicit def toSeries[V](l: List[(Double, V)]): Series[V] = {
    l.map( a => a match {
      case (t: Double, None) => (t, None)
      case (t: Double, v) => (t, Some(v))
    })
  }
}
