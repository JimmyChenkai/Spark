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

package org.apache.spark.sql.catalyst.rules

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.AtomicLongMap

import org.apache.spark.sql.catalyst.util.DateTimeUtils.NANOS_PER_SECOND

case class QueryExecutionMetering() {
  private val timeMap = AtomicLongMap.create[String]()
  private val numRunsMap = AtomicLongMap.create[String]()
  private val numEffectiveRunsMap = AtomicLongMap.create[String]()
  private val timeEffectiveRunsMap = AtomicLongMap.create[String]()

  /** 重置有关运行特定规则所花费时间的统计信息 */
  def resetMetrics(): Unit = {
    timeMap.clear()
    numRunsMap.clear()
    numEffectiveRunsMap.clear()
    timeEffectiveRunsMap.clear()
  }
  //总耗时
  def totalTime: Long = {
    timeMap.sum()
  }
  //运行行数耗时
  def totalNumRuns: Long = {
    numRunsMap.sum()
  }
  //执行耗时
  def incExecutionTimeBy(ruleName: String, delta: Long): Unit = {
    timeMap.addAndGet(ruleName, delta)
  }
  //有效执行耗时
  def incTimeEffectiveExecutionBy(ruleName: String, delta: Long): Unit = {
    timeEffectiveRunsMap.addAndGet(ruleName, delta)
  }
  //有效执行行数耗时
  def incNumEffectiveExecution(ruleName: String): Unit = {
    numEffectiveRunsMap.incrementAndGet(ruleName)
  }
  //执行行数耗时
  def incNumExecution(ruleName: String): Unit = {
    numRunsMap.incrementAndGet(ruleName)
  }

  /** 转储有关运行特定规则所花费时间的统计信息 */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxLengthRuleNames = if (map.isEmpty) {
      0
    } else {
      map.keys.map(_.toString.length).max
    }

    val colRuleName = "Rule".padTo(maxLengthRuleNames, " ").mkString
    val colRunTime = "Effective Time / Total Time".padTo(len = 47, " ").mkString
    val colNumRuns = "Effective Runs / Total Runs".padTo(len = 47, " ").mkString

    val ruleMetrics = map.toSeq.sortBy(_._2).reverseMap { case (name, time) =>
      val timeEffectiveRun = timeEffectiveRunsMap.get(name)
      val numRuns = numRunsMap.get(name)
      val numEffectiveRun = numEffectiveRunsMap.get(name)

      val ruleName = name.padTo(maxLengthRuleNames, " ").mkString
      val runtimeValue = s"$timeEffectiveRun / $time".padTo(len = 47, " ").mkString
      val numRunValue = s"$numEffectiveRun / $numRuns".padTo(len = 47, " ").mkString
      s"$ruleName $runtimeValue $numRunValue"
    }.mkString("\n", "\n", "")

    s"""
       |=== Metrics of Analyzer/Optimizer Rules ===
       |Total number of runs: $totalNumRuns
       |Total time: ${totalTime / NANOS_PER_SECOND.toDouble} seconds
       |
       |$colRuleName $colRunTime $colNumRuns
       |$ruleMetrics
     """.stripMargin
  }
}
