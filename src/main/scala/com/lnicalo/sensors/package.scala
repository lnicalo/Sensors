package com.lnicalo

package object sensors {
  type Value[V] = Option[V]
  type Series[V] = List[(Double, Option[V])]

  /* Implicit conversion to facilitate testing */
  implicit def toSeries[V](l: List[(Double, V)]) = {
    l.map( a => (a._1, Option(a._2)))
  }
}
