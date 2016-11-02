package com.lnicalo.sensors

/**
  * Created by LNICOLAS on 31/10/2016.
  */

import org.scalatest.{FunSuite, ShouldMatchers}

class TimeSeriesUtilsSuite extends FunSuite with ShouldMatchers {
  test("PairWiseOperation - product") {
    val v = List((1.0, 1),(2.0, 2),(3.0, 4),(4.0, 6)).sortBy(_._1)
    val w = List((1.5, 1),(2.5, 3),(3.5, 5),(4.5, 7)).sortBy(_._1)
    val out = Signal.PairWiseOperation(v, w){ (a: Int, b :Int) => a * b}
    val test_out = List((1.5, 1),(2.0, 2),(2.5, 6), (3.0, 12), (3.5, 20), (4.0, 30), (4.5, 42))
    out should be (test_out)
  }

  test("PairWiseOperation - tails") {
    val v = List((3.0, 4),(4.0, 6)).sortBy(_._1)
    val w = List((1.5, 1),(2.5, 3),(3.5, 5),(4.5, 7)).sortBy(_._1)
    val out = Signal.PairWiseOperation(v, w){ (a: Int, b :Int) => a * b}
    val test_out = List((3.0,12), (3.5,20), (4.0,30), (4.5,42))
    out should be (test_out)
  }

  test("PairWiseOperation - list with one element") {
    val v = List((3.0, 4)).sortBy(_._1)
    val w = List((1.5, 1),(2.5, 3),(3.5, 5),(4.5, 7)).sortBy(_._1)
    val out = Signal.PairWiseOperation(v, w){ (a: Int, b :Int) => a * b}
    val test_out = List((3.0,12), (3.5,20), (4.5,28))
    out should be (test_out)
  }

  test("PairWiseOperation - one empty list") {
    val v = List()
    val w = List((1.5, 1),(2.5, 3),(3.5, 5),(4.5, 7)).sortBy(_._1)
    val out = Signal.PairWiseOperation(v, w){ (a: Int, b :Int) => a * b}
    val test_out = Nil
    out should be (test_out)
  }

}
