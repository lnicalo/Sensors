package com.lnicalo.sensors


import org.scalatest.{FunSuite, ShouldMatchers}

class SplitOperationSuite extends FunSuite with ShouldMatchers {
  test("Split operation") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,6))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5, 7))
    val out = Signal.SplitOperation(v, w)

    val test_out = Map(
      1 -> List((1.5,Some(1)), (2.0,Some(2)), (2.5,None)),
      3 -> List((2.5,Some(2)), (3.0,Some(4)), (3.5,None)),
      5 -> List((3.5,Some(4)), (4.0,Some(6)), (4.5,None)),
      7 -> List((4.5,Some(6)), (4.5,None))
    )

    out should be (test_out)
  }

  test("Split operation - tails") {
    val v = List((3.0,4), (4.0,6))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,7))

    val out = Signal.SplitOperation(v, w)

    val test_out = Map(
      3 -> List((3.0,Some(4)), (3.5,None)),
      5 -> List((3.5,Some(4)), (4.0,Some(6)), (4.5,None)),
      7 -> List((4.5,Some(6)), (4.5,None))
    )

    out should be (test_out)
  }

  test("Split operation - list with one element") {
    val v = List((3.0, 4))
    val w = List((1.5,1), (2.5,3), (3.5,5), (4.5,7))
    val out = Signal.SplitOperation(v, w)

    val test_out = Map(
      3 -> List((3.0,Some(4)), (3.5,None)),
      5 -> List((3.5,Some(4)), (4.5,None)),
      7 -> List((4.5,Some(4)), (4.5,None))
    )
    out should be (test_out)
  }

  test("Split operation - with same timestamps") {
    val v = List((1.0, 1),(2.0, 2),(3.0, 4),(4.0, 6), (5.0, 8))
    val w = List((1.5, 1),(2.0, 2),(3.0, 5),(3.5, 7), (5.0, 8))
    val out = Signal.SplitOperation(v, w)

    val test_out = Map(
      1 -> List((1.5,Some(1)), (2.0,Some(2)), (2.0,None)),
      2 -> List((2.0,Some(2)), (3.0,Some(4)), (3.0,None)),
      5 -> List((3.0,Some(4)), (3.5,None)),
      7 -> List((3.5,Some(4)), (4.0,Some(6)), (5.0,Some(8)), (5.0,None)),
      8 -> List((5.0,Some(8)), (5.0, None))
    )

    out should be (test_out)
  }

  test("Split operation - with duplicates") {
    val v = List((1.0,1), (2.0,2), (3.0,4), (4.0,6), (4.9,8), (5.0,8), (10.0,8))
    val filter = List((1.5,true), (1.9,false), (3.0,true), (3.5,true), (5.0,false))

    val out = Signal.SplitOperation(v, filter)
    val test_out = Map(
      false -> List((1.9,Some(1)), (2.0,Some(2)), (3.0,Some(4)),
        (3.0,None), (5.0,Some(8)), (10.0,None)),
      true -> List((1.5,Some(1)), (1.9,None), (3.0,Some(4)),
        (4.0,Some(6)), (4.9,Some(8)), (5.0,None))
    )

    out should be (test_out)
  }
}
