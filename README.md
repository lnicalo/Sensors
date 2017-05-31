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

Even though, this approach is feasible for relatively small signals. It becomes really hard with big signals collected from long running sensor networks. Replicating samples multiplies the size of time series in memory that grows out of control quite quickly.

This library simplyfies and optimises the workflow with time series.

# Dependencies

The library sits on other excellent Java and Scala libraries.

- Apache Spark for distributed computation with in-memory capabilities.

# Functionality


