package com.lnicalo.sensors

import scala.annotation.tailrec

object TimeSeriesUtils {
  def PairWiseOperation[V,O](v: List[(Double, V)], w: List[(Double, V)])(f: (V,V) => O): List[(Double, O)] = {
    @tailrec
    def recursive(v: List[(Double, V)], w: List[(Double, V)], acc: List[(Double, O)]): List[(Double, O)] = {
      if (v.isEmpty || w.isEmpty) acc
      else {
        val d = w.head
        val e = v.head
        if (d._1 > e._1) {
          val s = (d._1, f(e._2, d._2))
          recursive(v, w.tail, s :: acc)
        } else {
          val s = (e._1, f(e._2, d._2))
          recursive(v.tail, w, s :: acc)
        }
      }
    }
    recursive(v.reverse, w.reverse, Nil)
  }
//  implicit class DoubleOps[V : Fractional](val num: V) extends AnyVal {
//    def +[K](that : Signal[K, V]) = that + num
//    def -[K](that : Signal[K, V]) = that.apply_operation(num) { (x,y) => implicitly[Fractional[V]] minus (y, x) }
    // def /[K](that : Signal[K, V]) = that.apply_operation(num) {(x,y) => y / x}
//    def *[K](that : Signal[K, V]) = that * num
//  }
}
