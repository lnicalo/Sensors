package com.lnicalo.sensors

import org.scalatest.{FunSuite, _}

import scala.collection.immutable.HashMap

/**
  * Created by LNICOLAS on 05/12/2016.
  */
class SignalOperationSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("span") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, Some(3.0)), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(3.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))
  }

  test("span with middle nones") {
    val s = List((1.0, Some(1.0)), (2.0, None), (3.0, Some(3.0)), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(2.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))
  }

  test("span with several ending nones") {
    val s = List((1.0, Some(1.0)), (2.0, Some(2.0)), (3.0, None), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(2.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(1.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(3.0)))
  }

  test("span with several starting nones") {
    val s = List((1.0, None), (2.0, None), (3.0, Some(3.0)), (4.0, None))

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(1.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> Some(3.0)))

    output = Signal.end(s)
    output should be(HashMap("End" -> Some(4.0)))
  }

  test("span with all nones") {
    val s = List((1.0, Some(2.0)), (2.0, None), (3.0, None), (4.0, None))
      .filter({ case (_, Some(_)) => false case _ => true })

    var output = Signal.duration(s)
    output should be(HashMap("Duration" -> Some(0.0)))

    output = Signal.start(s)
    output should be(HashMap("Start" -> None))

    output = Signal.end(s)
    output should be(HashMap("End" -> None))
  }
}