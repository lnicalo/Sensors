package com.lnicalo.sensors

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

import org.scalatest.{FunSuite, ShouldMatchers}
import com.lnicalo.sensors.Signal._

class OperationsWithFilteredSignalsSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("timings") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal
      .where(signal > 14)
      .timings()
      .toDataset

    output("1") should be (HashMap("Duration" -> None, "Start" -> None, "End" -> None))
    output("2") should be (HashMap("Start" -> Some(20.0), "End" -> Some(30.0), "Duration" -> Some(10.0)))
  }

  test("area with empty strings") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal
      .where(signal > 14)
      .span()
      .toDataset

    output("1") should be (HashMap("Duration" -> None, "Start" -> None, "End" -> None))
    output("2") should be (HashMap("Start" -> Some(20.0), "End" -> Some(30.0), "Duration" -> Some(10.0)))
  }
}