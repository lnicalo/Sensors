package com.lnicalo

package object sensors {
  type Value[V] = Option[V]
  type Series[V] = List[(Double, Option[V])]

  /* Implicit conversion to facilitate testing */
  implicit def toSeries[V](l: List[(Double, V)]): Series[V] = {
    l.map( a => a match {
      case (t: Double, None) => (t, None)
      case (t: Double, v: V) => (t, Some(v))
    })
  }
}
