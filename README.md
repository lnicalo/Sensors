[![Build Status](https://travis-ci.org/lnicalo/Sensors.svg?branch=master)](https://travis-ci.org/lnicalo/Sensors)
[![Coverage Status](https://coveralls.io/repos/github/lnicalo/Sensors/badge.svg?branch=master)](https://coveralls.io/github/lnicalo/Sensors?branch=master)
# Sensors
 
Scala package to process time series from different sensors with Spark

# Overview

Processing time series collected from different sensors poses several challenges as a result of data may not be aligned or have the same time sampling. Data cannot be writen in tabular form causing that writing data queries can be quite hard for data scientists. They need to deal with the misalignment all the time.

For example, you have the next two time series representing voltage and current of certain system:
~~~~
voltage = [(1, 2.0), (3, 3.0), (5, 5.0), (6, null)]
current = [(2, 2.0), (3, 3.0), (4, 5.0), (7, null)]
~~~~
We have represented signals as a list of tuples where the first element is the timestamp and the second element is the value. The first value at ``timestamp 1`` of ``voltage`` is ``2.0``, and then it changes to ``3.0`` at ``timestamp 3`` and so on.

As you can observe ``voltage`` and ``current`` samples are not aligned and the two time series do not even start at the same time. This is a very common problem in sensor networks where logging system do not report the current status of the sensors synchronously. This time series reflect the status of the sensor when it was logged. We assume that it stays unchanged until we get the next sample.

Data analyst may want to multiply these two time series to get the instantaneous power like this:

~~~~
voltage * current = [(2, 4.0), (3, 6.0), (4, 15.0), (5, 25.0), (6, null)]
~~~~

To do so, we could resample both time series as follows
~~~~
voltage = [(1, 2.0),  (2, 2.0), (3, 3.0), (4, 3.0), (5, 5.0), (6, null), (5, 5.0)]
current = [(1, null), (2, 2.0), (3, 3.0), (4, 5.0), (5, 5.0), (6, 5.0),  (7, null)]
~~~~
and easily multiply both to get the desired output.

Even though, this approach is feasible for relatively small signals. It becomes really hard with big signals collected from long running sensor networks and thousands of time series. Replicating samples multiplies the size of time series in memory that grows out of control quite quickly. Furthermore, the problem 

This library simplyfies and optimises the workflow with time series using parallel processing with Spark. All Spark are hidden to the user. Nevertheles, the developer can easly access Spark API to extend library capabilities on fly.

# Dependencies

The library sits on other excellent Java and Scala libraries.

- Apache Spark for distributed computation with in-memory capabilities.

# Functionality

## Creating signals

``Signal`` class is the main abstraction of the library. It is an extended RDD tailored for time series processing. Signal is essentially a parallel array of tuples. When collecting data from sensor network, you may have different trials, experiments, journeys, etc. 

For example, imagine you are logging data from a car and you vould like to collect the data by day or by journey. In this case, the key or identifier may be a string of the date or simply a identifier of the journey. The first element of the tuple correspond to the identifier of the journey and the second element is actually the time series data. Time series data is stored as a list of tuples as follows: 

```[(2, 4.0), (3, 6.0), (4, 15.0), (5, 25.0), (6, null)]```

So, you can create several signals objects to store different time series

#### From local variables
Signals can created from local variables for testing as follows:
```scala
val current = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (20.0, 20.0), (30.0, 30.0))) ))
```
#### From RDD
Signals can created from Spark RDD as follows:
```scala
val current = Signal(rdd)
```
RDD schema must be ```[K, List[(Double, V)]``` where K and V can be any type.

## Operations

#### Mathematical operations

Signals can be operating as any other fractional using a set of operators available:

```+, +:, |+|, -, -:, *, *:, /, /:```

```scala
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
```
__Note__: *As of this version, ``signal`` class do not show exactly as fractional. For example, API presents rare operators like ```+:``` or ```|+|```. There are plans to develop a more friendly API that presents the four operator: ```+, -, *, /```*

#### Agregation operations

Signals can be aggregated to extract statistics such as the first value, last value, start timestamp, last timestamp, duration, average, span, area under the curve. Method like lastValue are lazy evaluated and return the ``signal`` class to allow user to write script fluenty. Finally append ``.toDataset`` to run the code and get the output:

```scala
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
```

#### Interaction between signals

The library can be used to analyse interaction betwen signals.

##### Filtering
For example, we would like to see the average of the ``current`` when ``voltage`` is larger than 2.

```scala
val current = Signal(Array(
      ("1", List((1.0, 1.0), (2.0, 0.25), (3.0, 3.0))),
      ("2", List((10.0, 10.0), (15.0, 20.0), (30.0, 30.0))) ))
      
val voltage = Signal(Array(
  ("1", List((1.5, 2.0), (1.75, 0.5), (2.5, 0.0))),
  ("2", List((9.0, 12.0), (20.0, 20.0), (25.0, 30.0))) ))
  
val output = current
  .where(voltage > 2.0)
  .avg()
  .toDataset
```

##### Split signals
Signals can be split into several chunks based on the signal itself or other signals. This is particularly useful when we need to know the average voltage each time current is larger than 2. Note the difference with the filtering case. In this case, we are split each time series into different chunk based on other time series. Each chunk turns into a new time series where we can extract statistics from.

```scala
    val output = current
      .splitBy(voltage > 2.0)
      .lastValue()
      .firstValue()
      .start()
      .end()
      .toDataset
```

``Current`` signal may have been used to split the ``current`` signal itself to compute the average current when current is larger than 2.

```scala
val output = current
      .splitBy(current > 2.0)
      .avg()
      .toDataset
```
##### Bin signals
Signals can be binned based on other time series. This can be used when we would like to extract statistics grouped by values.

For example, we would like to know the time that ``current `` is larger than 2.0

```scala
val output = current
      .binBy(current > 2.0)
      .duration()
      .toDataset
```
