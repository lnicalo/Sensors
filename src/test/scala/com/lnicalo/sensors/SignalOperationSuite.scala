package com.lnicalo.sensors

import org.scalatest.{FunSuite, _}

import scala.collection.immutable.HashMap

/**
  * Created by LNICOLAS on 05/12/2016.
  */
class SignalOperationSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("time series with one none at end") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, Some(3.0)), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(3.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))

    output = Signal.firstValue(s)
    output should be(HashMap("First" -> Some(1.0)))

    output = Signal.lastValue(s)
    output should be(HashMap("Last" -> Some(3.0)))
  }

  test("time series with middle nones") {
    val s = List((1.0, Some(1.0)), (2.0, None), (3.0, Some(3.0)), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(2.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))

    output = Signal.firstValue(s)
    output should be(HashMap("First" -> Some(1.0)))

    output = Signal.lastValue(s)
    output should be(HashMap("Last" -> Some(3.0)))
  }

  test("time series with several ending nones") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, None), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(2.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(3.0)))

    output = Signal.firstValue(s)
    output should be(HashMap("First" -> Some(1.0)))

    output = Signal.lastValue(s)
    output should be(HashMap("Last" -> Some(2.0)))
  }

  test("time series with several starting nones") {
    val s = List((1.0,None), (2.0,None), (3.0,Some(3.0)), (4.0,None))
    //val r = List((1.5,None), (1.75,Some(1.0)), (2.0,None), (2.5, Some(0.25)), (3.0,Some(3.0)))
    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(1.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(3.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))

    output = Signal.firstValue(s)
    output should be(HashMap("First" -> Some(3.0)))

    output = Signal.lastValue(s)
    output should be(HashMap("Last" -> Some(3.0)))
  }

  test("time series with all nones") {
    val s = List((1.0, Some(2.0)), (2.0, None), (3.0, None), (4.0, None))
      .filter({ case (_, Some(_)) => false case _ => true })

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(0.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> None))

    output = Signal.end(s)
    output should be(HashMap("End" -> None))

    output = Signal.firstValue(s)
    output should be(HashMap("First" -> None))

    output = Signal.lastValue(s)
    output should be(HashMap("Last" -> None))
  }
}