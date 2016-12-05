package com.lnicalo

/**
  * Created by LNICOLAS on 05/12/2016.
  */
package object sensors {
  type Value[V] = Option[V]
  type Series[V] = List[(Double, Option[V])]

  /* Implicit conversion to facilitate testing */
  implicit def toSeries[V](l: List[(Double, V)]) = {
    l.map( a => (a._1, Option(a._2)))
  }
}
