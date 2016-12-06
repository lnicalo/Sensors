package com.lnicalo.sensors

/**
  * Created by LNICOLAS on 31/10/2016.
  */

import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.mutable.ArrayBuffer

class PairWiseOperationSuite extends FunSuite with ShouldMatchers {
  test("PairWiseOperation - product") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,6))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5, 7))
    val out = Signal.PairWiseOperation(v, w){
      (a,b) => (a,b) match {
        case (Some(x), Some(y)) => Option(x * y)
        case _ => None
      }
    }
    val test_out = toSeries(List((1.5,1), (2.0,2), (2.5,6), (3.0,12), (3.5, 20), (4.0,30), (4.5,42)))
    out should be (test_out)
  }

  test("PairWiseOperation - tails") {
    val v = List((3.0,4), (4.0,6))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,7))
    val out = Signal.PairWiseOperation(v, w){
      (a,b) => (a,b) match {
        case (Some(x), Some(y)) => Option(x * y)
        case _ => None
      }
    }
    val test_out = toSeries(List((3.0,12), (3.5,20), (4.0,30), (4.5,42)))
    out should be (test_out)
  }

  test("PairWiseOperation - list with one element") {
    val v = List((3.0, 4))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,7))
    val out = Signal.PairWiseOperation(v, w){
      (a,b) => (a,b) match {
        case (Some(x), Some(y)) => Option(x * y)
        case _ => None
      }
    }
    val test_out = toSeries(List((3.0,12), (3.5,20), (4.5,28)))
    out should be (test_out)
  }

  test("PairWiseOperation - with same timestamps") {
    val v = List((1.0, 1),(2.0, 2),(3.0, 4),(4.0, 6), (5.0, 8))
    val w = List((1.5, 1),(2.0, 2),(3.0, 5),(3.5, 7), (5.0, 8))
    val out = Signal.PairWiseOperation(v, w){
      (a,b) => (a,b) match {
        case (Some(x), Some(y)) => Option(x * y)
        case _ => None
      }
    }
    val test_out = toSeries(List((1.5,1), (2.0,4), (3.0,20), (3.5,28), (4.0,42), (5.0,64)))
    out should be (test_out)
  }

  test("FilterOperation - with same timestamps") {
    val v = List((1.0,1), (2.0,2), (3.0, 4), (4.0,6), (5.0,8), (10.0,8))
    val filter = List((1.5,true), (1.9,false), (3.0,true), (3.5,false), (5.0,true))
    val out = Signal.PairWiseFilter(v, filter)
    val test_out = List((1.5,Some(1)), (1.9,None), (2.0,None), (3.0,Some(4)),
      (3.5,None), (4.0,None), (5.0,Some(8)), (10.0,Some(8)))
    out should be (test_out)
  }

  test("FilterOperation - with duplicates") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,6), (4.9,8), (10.0,8))
    val filter = List((1.5,true), (1.9,false), (3.0,true), (3.5,true), (5.0,false))
    val out = Signal.PairWiseFilter(v, filter)
    val test_out = List((1.5,Some(1)), (1.9,None), (2.0,None), (3.0,Some(4)), (3.5,Some(4)),
      (4.0,Some(6)), (4.9,Some(8)), (5.0,None), (10.0,None))
    out should be (test_out)
  }

}
