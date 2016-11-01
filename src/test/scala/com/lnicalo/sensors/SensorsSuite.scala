package com.lnicalo.sensors

/**
  * Created by LNICOLAS on 31/10/2016.
  */

import org.apache.spark.{SparkConf, SparkContext}
import TimeSeriesUtils.DoubleOps

import org.scalatest.{FunSuite, ShouldMatchers}

class SensorsSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
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

  test("apply operation") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal1 = Signal(Array(("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
                               ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
    val signal2 = Signal(Array(("1", List((1.5, 1.5), (2.5, 2.5), (3.5, 3.5))),
                               ("2", List((10.5, 10.5), (20.5, 20.5), (30.5, 30.5))) ))

    val output = (signal1 + signal2).collectAsMap()
    output("1") should be (List((1.5,2.5), (2.0,3.5), (2.5,4.5), (3.0,5.5), (3.5,6.5)))
    output("2") should be (List((10.5,20.5), (20.0,30.5), (20.5,40.5), (30.0,50.5), (30.5,60.5)))
  }

  test("operators with constants") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    sc = new SparkContext(conf)

    val signal = Signal(Array(("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))

    var output = (3 * signal).collectAsMap()
    output("1") should be (List((1.0, 3.0), (2.0, 6.0), (3.0, 9.0)))
    output("2") should be (List((10.0, 30.0), (20.0, 60.0), (30.0, 90.0)))

    output = (3 - signal).collectAsMap()
    output("1") should be (List((1.0, 2.0), (2.0, 1.0), (3.0, 0.0)))
    output("2") should be (List((10.0, -7.0), (20.0, -17.0), (30.0, -27.0)))

    output = (3 - 2 * signal).collectAsMap()
    output("1") should be (List((1.0, 1.0), (2.0, -1.0), (3.0, -3.0)))
    output("2") should be (List((10.0, -17.0), (20.0, -37.0), (30.0, -57.0)))
  }

}
