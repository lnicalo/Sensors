package com.lnicalo.sensors

import scala.annotation.tailrec

object TimeSeriesUtils {
  def PairWiseOperation[T](v: List[(Double, T)], w: List[(Double, T)])(f: (T,T) => T): List[(Double, T)] = {
    @tailrec
    def recursive(v: List[(Double, T)], w: List[(Double, T)], acc: List[(Double, T)]): List[(Double, T)] = {
      if (v.isEmpty || w.isEmpty) acc
      else {
        val d = w.head
        val e = v.head
        if (d._1 > e._1) {
          val s: (Double, T) = (d._1, f(e._2, d._2))
          recursive(v, w.tail, s :: acc)
        } else {
          val s: (Double, T) = (e._1, f(e._2, d._2))
          recursive(v.tail, w, s :: acc)
        }
      }
    }
    recursive(v.reverse, w.reverse, Nil)
  }

  implicit class DoubleOps(val double: Double) extends AnyVal {
    def +[K](that : Signal[K]) = that + double
    def -[K](that : Signal[K]) = that.apply_operation(double) {(x,y) => y - x}
    def /[K](that : Signal[K]) = that.apply_operation(double) {(x,y) => y / x}
    def *[K](that : Signal[K]) = that * double
  }
}
