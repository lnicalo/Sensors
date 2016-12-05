package com.lnicalo.sensors

import org.scalatest.{FunSuite, _}

import scala.collection.immutable.HashMap

/**
  * Created by LNICOLAS on 05/12/2016.
  */
class MathSignalFunctionSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("span") {
    val s = List((1.0,Some(1.0)), (2.0,Some(2.0)), (3.0,Some(3.0)), (4.0,None))

    val output = MathSignalFunctions.span(s)
    output should be (HashMap("First" -> Some(1.0),
      "Last" -> Some(3.0), "Span" -> Some(2.0)))
  }

  test("span with middle nones") {
    val s = List((1.0,Some(1.0)), (2.0,None), (3.0,Some(3.0)), (4.0,None))

    val output = MathSignalFunctions.span(s)
    output should be (HashMap("First" -> Some(1.0),
      "Last" -> Some(3.0), "Span" -> Some(2.0)))
  }

  test("span with several ending nones") {
    val s = List((1.0,Some(1.0)), (2.0,Some(2.0)), (3.0,None), (4.0,None))

    val output = MathSignalFunctions.span(s)
    output should be (HashMap("First" -> Some(1.0),
      "Last" -> Some(2.0), "Span" -> Some(1.0)))
  }

  test("span with several starting nones") {
    val s = List((1.0,None), (2.0,None), (3.0,Some(3.0)), (4.0,None))

    val output = MathSignalFunctions.span(s)
    output should be (HashMap("First" -> Some(3.0),
      "Last" -> Some(3.0), "Span" -> Some(0.0)))
  }

  test("span with all nones") {
    val s = List((1.0,Some(2.0)), (2.0,None), (3.0,None), (4.0,None))
      .filter({case (_, Some(_)) => false case _ => true})
    val output = MathSignalFunctions.span(s)
    output should be (HashMap("First" -> None,
      "Last" -> None, "Span" -> None))
  }
}