---
layout: global
displayTitle: Using Spark's "Hadoop Free" Build
title: Using Spark's "Hadoop Free" Build
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Spark uses Hadoop client libraries for HDFS and YARN. Starting in version Spark 1.4, the project packages "Hadoop free" builds that lets you more easily connect a single Spark binary to any Hadoop version. To use these builds, you need to modify `SPARK_DIST_CLASSPATH` to include Hadoop's package jars. The most convenient place to do this is by adding an entry in `conf/spark-env.sh`.

This page describes how to connect Spark to Hadoop for different types of distributions.

# Apache Hadoop
For Apache distributions, you can use Hadoop's 'classpath' command. For instance:

{% highlight bash %}
### in conf/spark-env.sh ###

# If 'hadoop' binary is on your PATH
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

# With explicit path to 'hadoop' binary
export SPARK_DIST_CLASSPATH=$(/path/to/hadoop/bin/hadoop classpath)

# Passing a Hadoop configuration directory
export SPARK_DIST_CLASSPATH=$(hadoop --config /path/to/configs classpath)

{% endhighlight %}
