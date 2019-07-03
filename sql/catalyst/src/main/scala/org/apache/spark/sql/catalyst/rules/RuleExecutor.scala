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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object RuleExecutor {
  protected val queryExecutionMeter = QueryExecutionMetering()

  /** 转储有关运行特定规则所花费时间的统计信息 */
  def dumpTimeSpent(): String = {
    queryExecutionMeter.dumpTimeSpent()
  }

  /** 重置有关运行特定规则所花费时间的统计信息 */
  def resetMetrics(): Unit = {
    queryExecutionMeter.resetMetrics()
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * 表示最大执行次数的规则的执行策略。
   * 如果执行在最大化之前达到固定点（i.e. converge），它将停止。
   */
  abstract class Strategy { def maxIterations: Int }

  /** 只执行一次执行策略 */
  case object Once extends Strategy { val maxIterations = 1 }

  /** 固定的点或者最大值无论哪个先出现，就执行策略 */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** 批量rule */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** 批量rule集合, 可以被继承子类覆盖 */
  protected def batches: Seq[Batch]

  /**
   * 定义检查函数，该函数在执行每个规则后检查计划的结构完整性
   * 例如, 我们可以在“优化器”中的每个规则之后检查计划是否仍然得到解析，
   * 所以我们可以捕获返回无效计划的规则。
   * 如果给定的计划未通过结构完整性检查，则检查函数返回“false”。
   */
  protected def isPlanIntegral(plan: TreeType): Boolean = true

  /**
   * 执行子类定义的批量rule，同时记录每个rule的运行时间信息
   * @see [[execute]]
   */
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType = {
    QueryPlanningTracker.withTracker(tracker) {
      execute(plan)
    }
  }

  /**
   * 执行子类定义的批量rule， 使用定义的执行策略来串行执行批量rule
   * 每个单独rule内部也是串行执行
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get

    // 根据初始输入运行结构完整性检查器
    if (!isPlanIntegral(plan)) {
      val message = "The structural integrity of the input plan is broken in " +
        s"${this.getClass.getName.stripSuffix("$")}."
      throw new TreeNodeException(plan, message, null)
    }

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // 运行到固定点（或策略中指定的最大迭代次数）。
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan)

            if (effective) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              planChangeLogger.logRule(rule.ruleName, plan, result)
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // 使用 QueryPlanningTracker记录运行时间
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            // 根据每个规则后的计划运行结构完整性检查器。
            if (!isPlanIntegral(result)) {
              val message = s"After applying rule ${rule.ruleName} in batch ${batch.name}, " +
                "the structural integrity of the plan is broken."
              throw new TreeNodeException(result, message, null)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // 当这一规则执行多次时候，会记录
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            if (Utils.isTesting) {
              throw new TreeNodeException(curPlan, message, null)
            } else {
              logWarning(message)
            }
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)
    }

    curPlan
  }

  //内部类  修改日志级别的
  private class PlanChangeLogger {

    private val logLevel = SQLConf.get.optimizerPlanChangeLogLevel

    private val logRules = SQLConf.get.optimizerPlanChangeRules.map(Utils.stringToSeq)

    private val logBatches = SQLConf.get.optimizerPlanChangeBatches.map(Utils.stringToSeq)

    def logRule(ruleName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
      if (logRules.isEmpty || logRules.get.contains(ruleName)) {
        def message(): String = {
          s"""
             |=== Applying Rule ${ruleName} ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
        }

        logBasedOnLevel(message)
      }
    }

    def logBatch(batchName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
      if (logBatches.isEmpty || logBatches.get.contains(batchName)) {
        def message(): String = {
          if (!oldPlan.fastEquals(newPlan)) {
            s"""
               |=== Result of Batch ${batchName} ===
               |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
          """.stripMargin
          } else {
            s"Batch ${batchName} has no effect."
          }
        }

        logBasedOnLevel(message)
      }
    }

    private def logBasedOnLevel(f: => String): Unit = {
      logLevel match {
        case "TRACE" => logTrace(f)
        case "DEBUG" => logDebug(f)
        case "INFO" => logInfo(f)
        case "WARN" => logWarning(f)
        case "ERROR" => logError(f)
        case _ => logTrace(f)
      }
    }
  }
}
