/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetrics


/**
 *`seq`可能不是可序列化的，理想情况下我们不应该发送`rows`和`unsafeRows`执行人。因此将它们标记为瞬态。
 */
case class LocalTableScanExec(
    output: Seq[Attribute],
    @transient rows: Seq[InternalRow]) extends LeafExecNode with InputRDDCodegen {
  //重写了metircs    
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
  //懒加载机制
  @transient private lazy val unsafeRows: Array[InternalRow] = {
    //如果rows是空值，返回空Array  
    if (rows.isEmpty) {
      Array.empty
    //否则UnsageProjection创建    
    } else {
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }
  //懒加载机制 获取分区树
  private lazy val numParallelism: Int = math.min(math.max(unsafeRows.length, 1),
    sqlContext.sparkContext.defaultParallelism)
  //RDD变量载体
  private lazy val rdd = sqlContext.sparkContext.parallelize(unsafeRows, numParallelism)
  //重写doExecute方法，返回一个RDD[InternalRow]
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }
  //覆盖重写StringArgs
  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }
  //重写了executeCollect
  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows").add(unsafeRows.size)
    unsafeRows
  }
  //覆盖重写了executeTake
  override def executeTake(limit: Int): Array[InternalRow] = {
    val taken = unsafeRows.take(limit)
    longMetric("numOutputRows").add(taken.size)
    taken
  }

  // 输入已经是UnsafeRows。
  override protected val createUnsafeProjection: Boolean = false

  // 当没有父代时，不要编码 - 支持快速驱动程序本地收集/获取路径。
  override def supportCodegen: Boolean = (parent != null)

  override def inputRDD: RDD[InternalRow] = rdd
}
