package com.lnicalo.sensors

import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.mutable.ListBuffer

class SplitOperationSuite extends FunSuite with ShouldMatchers {
  test("Split operation") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,None))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5, None))
    val out = Signal.SplitOperation(v, w)

    val test_out = List(
      ((3.5,4.0),ListBuffer((3.5,Some(4)), (4.0,None))),
      ((1.5,2.5),ListBuffer((1.5,Some(1)), (2.0,Some(2)), (2.5,None))),
      ((2.5,3.5),ListBuffer((2.5,Some(2)), (3.0,Some(4)), (3.5,None))))

    out should be (test_out)
  }

  test("Bin operation - tails") {
    val v = List((3.0,4), (4.0,None))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,None))

    val out = Signal.SplitOperation(v, w)

    val test_out = List(
      ((3.5,4.0),ListBuffer((3.5,Some(4)), (4.0,None))),
      ((3.0,3.5),ListBuffer((3.0,Some(4)), (3.5,None)))
    )

    out should be (test_out)
  }

  test("Bin operation - list with one element") {
    val v = List((3.0, 4), (3.0, None))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,None))
    val out = Signal.SplitOperation(v, w)

    val test_out = List(((3.0,3.0),ListBuffer((3.0,Some(4)), (3.0,None))))
    out should be (test_out)
  }

  test("Bin operation - with same timestamps") {
    val v = List((1.0, 1),(2.0, 2),(3.0, 4),(4.0, 6), (5.0, None))
    val w = List((1.5, 1),(2.0, 2),(3.0, 5),(3.5, 7), (5.0, None))
    val out = Signal.SplitOperation(v, w)

    val test_out = List(
      ((2.0,3.0),ListBuffer((2.0,Some(2)), (3.0,Some(4)), (3.0,None))),
      ((3.0,3.5),ListBuffer((3.0,Some(4)), (3.5,None))),
      ((3.5,5.0),ListBuffer((3.5,Some(4)), (4.0,Some(6)), (5.0,None))),
      ((1.5,2.0),ListBuffer((1.5,Some(1)), (2.0,Some(2)), (2.0,None))))

    out should be (test_out)
  }

  test("Bin operation - with duplicates") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,6), (4.9,8), (5.0,8), (10.0,None))
    val filter = List((1.5,true), (1.9,false), (3.0,true), (3.5,true), (5.0,None))

    val out = Signal.SplitOperation(v, filter)
    val test_out = List(
      ((1.9,3.0),ListBuffer((1.9,Some(1)), (2.0,Some(2)), (3.0,Some(4)), (3.0,None))),
      ((1.5,1.9),ListBuffer((1.5,Some(1)), (1.9,None))),
      ((3.0,5.0),ListBuffer((3.0,Some(4)), (4.0,Some(6)), (4.9,Some(8)), (5.0,None))))
    out should be (test_out)
  }

  test("Bin operation - with nones in the middle") {
    val v = List((1.0,1), (2.0,None), (3.0,4), (4.0,None), (4.9,None), (5.0,8), (10.0,None))
    val filter = List((1.5,true), (1.9,false), (3.0,true), (3.5,true), (5.0,None))
    val out = Signal.SplitOperation(v, filter)

    val test_out = List(
      ((1.9,2.0),ListBuffer((1.9,Some(1)), (2.0,None))),
      ((3.0,3.0),ListBuffer((3.0,Some(4)), (3.0,None))),
      ((1.5,1.9),ListBuffer((1.5,Some(1)), (1.9,None))),
      ((3.0,4.0),ListBuffer((3.0,Some(4)), (4.0,None))),
      ((5.0,5.0),ListBuffer((5.0,Some(8)), (5.0,None))))

    out should be (test_out)
  }
}
