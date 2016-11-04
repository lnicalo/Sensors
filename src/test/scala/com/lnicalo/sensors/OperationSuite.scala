package com.lnicalo.sensors

/**
  * Created by LNICOLAS on 04/11/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

import org.scalatest.{FunSuite, ShouldMatchers}

class OperationSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("timings") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = GetTimings(signal).collectAsMap()
    output("1") should be (HashMap("Start" -> 1.0, "End" -> 3.0, "Duration" -> 2.0))
    output("2") should be (HashMap("Start" -> 10.0, "End" -> 30.0, "Duration" -> 20.0))
  }

  test("timings with filters") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
      ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))
    val output = GetTimings(signal1.where(signal1 > signal2).flatten()).collectAsMap()
    output(("1", (1.75, 2.0))) should be (HashMap("Start" -> 1.75, "End" -> 2.0, "Duration" -> 0.25))
    output(("1", (2.5, 3.0))) should be (HashMap("Start" -> 2.5, "End" -> 3.0, "Duration" -> 0.5))
    output(("2", (15, 20.0))) should be (HashMap("Start" -> 15.0, "End" -> 20.0, "Duration" -> 5.0))
  }
}
