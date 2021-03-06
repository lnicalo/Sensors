package com.lnicalo.sensors

/**
  * Created by LNICOLAS on 31/10/2016.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
//import TimeSeriesUtils.DoubleOps

import org.scalatest.{FunSuite, ShouldMatchers}

class SignalSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("random sample") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val lengths = Array(3,3,3)
    val signal = Signal.uniform_random_sample(lengths)

    val output = signal.collectAsMap
    output("1").length should be (3)
    output("2").length should be (3)
    output("3").length should be (3)
  }

  test("compare signal with strings") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, "off"), (2.0, "on"), (3.0, "on"))),
      ("2", List((10.0, "off"), (20.0, "on"), (30.0, "off"))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, "on"), (2.5, "off"), (3.5, "off"))),
      ("2", List((10.5, "on"), (20.5, "off"), (30.5, "on"))) ))

    val output = (signal1 |==| signal2).collectAsMap()
    output("1") should be (toSeries(List((1.5, false), (2.0, true), (2.5, false), (3.5, false))))
    output("2") should be (toSeries(List((10.5, false), (20.0, true), (20.5, false), (30.0, true), (30.5, false))))
  }

  test("compare signal with constant string") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, "off"), (2.0, "on"), (3.0, "on"))),
      ("2", List((10.0, "off"), (20.0, "on"), (30.0, "off"))) ))

    val output = (signal1 |==| "on").collectAsMap()
    output("1") should be (toSeries(List((1.0, false), (2.0, true), (3.0, true))))
    output("2") should be (toSeries(List((10.0, false), (20.0, true), (30.0, false))) )
  }

  test("math operations between signals") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 4.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 40.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (2.5, 4.0), (3.5, 5.0))),
      ("2", List((10.5, 10.5), (20.5, 20.5), (30.5, 30.5))) ))

    val tmp1 = (3 *: (100 /: signal1 - signal1 / 2.0 * 4.0)) * signal1 / 2.0
    val tmp2 = - (3 +: signal2) - 5 |+| signal1 |+| 4
    val tmp3 = tmp1 / signal1
    val output = (tmp1 - tmp2 |+| tmp3).collectAsMap()
    output("1") should be (toSeries(
      List((1.5,299.0), (2.0,211.0), (2.5,213.0), (3.0,131.5), (3.5,132.5)) ))
    output("2") should be (toSeries(
      List((10.5,-160.5), (20.0,-1108.0), (20.5,-1098.0), (30.0,-4781.75), (30.5,-4771.75))))
  }

  test("math operations with constants") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))

    var output = (3 -: signal).collectAsMap()
    output("1") should be (toSeries(List((1.0, 2.0), (2.0, 1.0), (3.0, 0.0))))
    output("2") should be (toSeries(List((10.0, -7.0), (20.0, -17.0), (30.0, -27.0))))

    output = ((3 -: 2 *: signal) / 2).collectAsMap()
    output("1") should be (toSeries(List((1.0, 0.5), (2.0, -0.5), (3.0, -1.5))))
    output("2") should be (toSeries(List((10.0, -8.5), (20.0, -18.5), (30.0, -28.5))))

    output = (-signal).collectAsMap()
    output("1") should be (toSeries(List((1.0, -1.0), (2.0, -2.0), (3.0, -3.0))))
    output("2") should be (toSeries(List((10.0, -10.0), (20.0, -20.0), (30.0, -30.0))))
  }

  test("boolean operations between signals") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 1.5), (2.5, 2.0), (3.5, 3.5))),
      ("2", List((10.5, 10.5), (20.5, 1.5), (30.5, -30.5))) ))

    var output = (signal1 > signal2).collectAsMap()
    output("1") should be (toSeries(List((1.5,false), (2.0,true), (2.5,false), (3.0,true), (3.5,false))))
    output("2") should be (toSeries(List((10.5,false), (20.0,true), (30.5,true))))

    output = (signal1 |==| signal2).collectAsMap()
    output("1") should be (toSeries(List((1.5,false), (2.5,true), (3.0,false), (3.5,false))))
    output("2") should be (toSeries(List((10.5,false), (30.5,false))))

    output = (signal1 <= signal2).collectAsMap()
    output("1") should be (toSeries(List((1.5,true), (2.0,false), (2.5,true), (3.0,false), (3.5,true))))
    output("2") should be (toSeries(List((10.5,true), (20.0,false), (30.5,false))))

    output = ((signal1 |==| signal2) and (signal1 >= 3.0)).collectAsMap()
    output("1") should be (toSeries(List((1.5,false), (3.5,false))))
    output("2") should be (toSeries(List((10.5,false), (30.5,false))))

    output = ((signal1 < signal2 / 2) and !(signal1 > signal2)).collectAsMap()
    output("1") should be (toSeries(List((1.5,false), (3.5,false))))
    output("2") should be (toSeries(List((10.5,false), (30.5,false))))

    output = (!(signal1 >= signal2) or (signal1 < 2.0)).collectAsMap()
    output("1") should be (toSeries(List((1.5,true), (2.0,false), (3.5,true))))
    output("2") should be (toSeries(List((10.5,true), (20.0,false), (30.5,false))))
  }

  test("boolean operation with constants") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))

    var output = (signal > 2).collectAsMap()
    output("1") should be (toSeries(List((1.0, false), (2.0, false), (3.0, true))))
    output("2") should be (toSeries(List((10.0, true), (20.0, true), (30.0, true))))

    output = (signal <= 2).collectAsMap()
    output("1") should be (toSeries(List((1.0, true), (2.0, true), (3.0, false))))
    output("2") should be (toSeries(List((10.0, false), (20.0, false), (30.0, false))))

    output = (signal |==| 2).collectAsMap()
    output("1") should be (toSeries(List((1.0, false), (2.0, true), (3.0, false))))
    output("2") should be (toSeries(List((10.0, false), (20.0, false), (30.0, false))))
  }

  test("operations without filter") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val output = signal
      .lastValue()
      .firstValue()
      .start()
      .end()
      .duration()
      .avg()
      .span()
      .area()
      .toDataset

    output("1") should be (HashMap("Area" -> Some(3.0), "Duration" -> Some(2.0),
      "Span" -> Some(2.0), "Start" -> Some(1.0), "Avg" -> Some(1.5),
      "Last" -> Some(3.0), "End" -> Some(3.0), "First" -> Some(1.0)))
    output("2") should be (HashMap("Area" -> Some(300.0), "Duration" -> Some(20.0),
      "Span" -> Some(20.0), "Start" -> Some(10.0), "Avg" -> Some(15.0),
      "Last" -> Some(30.0), "End" -> Some(30.0), "First" -> Some(10.0)))
  }

  test("operations with filter") {
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
      .firstValue()
      .start()
      .end()
      .duration()
      .avg()
      .span()
      .area()
      .toDataset

    output("1") should be (HashMap("Area" -> Some(0.375), "Duration" -> Some(0.75),
      "Span" -> Some(2.0), "Start" -> Some(1.75), "Avg" -> Some(0.5),
      "Last" -> Some(3.0), "End" -> Some(3.0), "First" -> Some(1.0)))

    output("2") should be (HashMap("Area" -> Some(100.0), "Duration" -> Some(5.0),
      "Span" -> Some(0.0), "Start" -> Some(15.0), "Avg" -> Some(20.0),
      "Last" -> Some(20.0), "End" -> Some(20.0), "First" -> Some(20.0)))
  }

  test("bin by") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
      ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))

    val output = signal1
      .binBy(signal1 > signal2)
      .lastValue()
      .firstValue()
      .start()
      .end()
      .toDataset

    output(("1", true)) should be (
      Map("Start" -> Some(1.75), "Last" -> Some(3.0), "End" -> Some(3.0), "First" -> Some(1.0))
    )

    output(("1", false)) should be (
      Map("Start" -> Some(1.5), "Last" -> Some(0.25), "End" -> Some(2.5), "First" -> Some(1.0))
    )

    output(("2", true)) should be (
      Map("Start" -> Some(15.0), "Last" -> Some(20.0), "End" -> Some(20.0), "First" -> Some(20.0))
    )

    output(("2", false)) should be (
      Map("Start" -> Some(10.0), "Last" -> Some(30.0), "End" -> Some(30.0), "First" -> Some(10.0))
    )
  }

  test("split by") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(
      ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
      ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))

    val output = signal1
      .splitBy(signal1 > signal2)
      .lastValue()
      .firstValue()
      .start()
      .end()
      .toDataset

    output(("1",(1.5,1.75))) should be (
      Map("Start" -> Some(1.5), "Last" -> Some(1.0), "End" -> Some(1.75), "First" -> Some(1.0)))

    output(("1",(2.0,2.5))) should be (
      Map("Start" -> Some(2.0), "Last" -> Some(0.25), "End" -> Some(2.5), "First" -> Some(0.25)))

    output(("2",(15.0,20.0))) should be (
      Map("Start" -> Some(15.0), "Last" -> Some(20.0), "End" -> Some(20.0), "First" -> Some(20.0)))

    output(("1",(1.75,3.0))) should be (
      Map("Start" -> Some(1.75), "Last" -> Some(3.0), "End" -> Some(3.0), "First" -> Some(1.0)))

    output(("2",(10.0,30.0))) should be (
      Map("Start" -> Some(10.0), "Last" -> Some(30.0), "End" -> Some(30.0), "First" -> Some(10.0)))
  }
}
