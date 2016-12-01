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
    val output = signal
      .timings()
      .toDataset
      .collectAsMap()
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
    val output = signal1
      .where(signal1 > signal2)
      .timings()
      .toDataset
      .collectAsMap()
    output(("1", (1.75, 2.0))) should be (HashMap("Start" -> 1.75, "End" -> 2.0, "Duration" -> 0.25))
    output(("1", (2.5, 3.0))) should be (HashMap("Start" -> 2.5, "End" -> 3.0, "Duration" -> 0.5))
    output(("2", (15, 20.0))) should be (HashMap("Start" -> 15.0, "End" -> 20.0, "Duration" -> 5.0))
  }

  test("last with filters") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
      ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))
    val output = signal1
      .where(signal1 > signal2)
      .lastValue()
      .toDataset
      .collectAsMap()
    output(("1", (1.75, 2.0))) should be (HashMap("Last" -> 1.0))
    output(("1", (2.5, 3.0))) should be (HashMap("Last" -> 0.25))
    output(("2", (15, 20.0))) should be (HashMap("Last" -> 20.0))
  }

  test("weighted average") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal.avg().toDataset.collectAsMap()
    output("1") should be (HashMap("Avg" -> 1.5))
    output("2") should be (HashMap("Avg" -> 15.0))
  }

  test("area") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal.area().toDataset.collectAsMap()
    output("1") should be (HashMap("Area" -> 3.0))
    output("2") should be (HashMap("Area" -> 300.0))
  }

  test("add op") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal
        .span()
        .area()
        .avg()
        .toDataset
        .collectAsMap()

    output("1") should be (HashMap("Area" -> 3.0, "Span" -> 1.0, "Avg" -> 1.5, "Last" -> 2.0, "First" -> 1.0))
    output("2") should be (HashMap("Area" -> 300.0, "Span" -> 10.0, "Avg" -> 15.0, "Last" -> 20.0, "First" -> 10.0))
  }

  test("add op with filters") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
      ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))
    val output = signal1
      .where(signal1 > signal2)
      .span()
      .area()
      .avg()
      .toDataset
      .collectAsMap()
    output(("1", (1.75, 2.0))) should be (
      HashMap("Area" -> 0.25, "Span" -> 0.0, "Avg" -> 1.0, "Last" -> 1.0, "First" -> 1.0))
    output(("1", (2.5, 3.0))) should be (
      HashMap("Area" -> 0.125, "Span" -> 0.0, "Avg" -> 0.25, "Last" -> 0.25, "First" -> 0.25))
    output(("2", (15, 20.0))) should be (
      HashMap("Area" -> 100.0, "Span" -> 0.0, "Avg" -> 20.0, "Last" -> 20.0, "First" -> 20.0))
  }
}
