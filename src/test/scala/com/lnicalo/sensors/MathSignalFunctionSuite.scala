package com.lnicalo.sensors

import org.scalatest.{FunSuite, _}

import scala.collection.immutable.HashMap

/**
  * Created by LNICOLAS on 05/12/2016.
  */
class MathSignalFunctionSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("time series with one none at end") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, Some(3.0)), (4.0, None))

    var output = MathSignalFunctions.avg(s)
    output should be(HashMap("Avg" -> Some(2.0)))

    output = MathSignalFunctions.area(s)
    output should be(HashMap("Area" -> Some(6.0)))

    output = MathSignalFunctions.span(s)
    output should be(HashMap("Span" -> Some(2.0)))
  }

  test("time series with middle nones") {
    val s = List((1.0, Some(1.0)), (2.0, None), (3.0, Some(3.0)), (4.0, None))

    var output = MathSignalFunctions.avg(s)
    output should be(HashMap("Avg" -> Some(2.0)))

    output = MathSignalFunctions.area(s)
    output should be(HashMap("Area" -> Some(4.0)))

    output = MathSignalFunctions.span(s)
    output should be(HashMap("Span" -> Some(2.0)))
  }

  test("time series with several ending nones") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, None), (4.0, None))

    var output = MathSignalFunctions.avg(s)
    output should be(HashMap("Avg" -> Some(1.5)))

    output = MathSignalFunctions.area(s)
    output should be(HashMap("Area" -> Some(3.0)))

    output = MathSignalFunctions.span(s)
    output should be(HashMap("Span" -> Some(1.0)))
  }

  test("time series with several starting nones") {
    val s = List((1.0, None), (2.0, None), (3.0, Some(3.0)), (4.0, None))

    var output = MathSignalFunctions.avg(s)
    output should be(HashMap("Avg" -> Some(3.0)))

    output = MathSignalFunctions.area(s)
    output should be(HashMap("Area" -> Some(3.0)))

    output = MathSignalFunctions.span(s)
    output should be(HashMap("Span" -> Some(0.0)))
  }

  test("time series with all nones") {
    val s = List((1.0, Some(2.0)), (2.0, None), (3.0, None), (4.0, None))
      .filter({ case (_, Some(_)) => false case _ => true })

    var output = MathSignalFunctions.avg(s)
    output should be(HashMap("Avg" -> None))

    output = MathSignalFunctions.area(s)
    output should be(HashMap("Area" -> None))

    output = MathSignalFunctions.span(s)
    output should be(HashMap("Span" -> None))
  }
}