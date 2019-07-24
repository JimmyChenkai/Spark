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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * 从计划中替换[[ ResolvedHint ]]运算符。将[[ HintInfo ]] 移动到关联的[[ Join ]]运算符，
 * 否则如果没有[[ Join ]]运算符匹配则删除它。
 */
object EliminateResolvedHint extends Rule[LogicalPlan] {

  private val hintErrorHandler = SQLConf.get.hintErrorHandler

  //这也是在优化阶段开始时调用的结果
  //正在使用transformUp而不是resolveOperators。
  def apply(plan: LogicalPlan): LogicalPlan = {
    val pulledUp = plan transformUp {
      case j: Join if j.hint == JoinHint.NONE =>
        val (newLeft, leftHints) = extractHintsFromPlan(j.left)
        val (newRight, rightHints) = extractHintsFromPlan(j.right)
        val newJoinHint = JoinHint(mergeHints(leftHints), mergeHints(rightHints))
        j.copy(left = newLeft, right = newRight, hint = newJoinHint)
    }
    pulledUp.transformUp {
      case h: ResolvedHint =>
        hintErrorHandler.joinNotFoundForJoinHint(h.hints)
        h.child
    }
  }

  /**
   * 将[[ HintInfo ]] 列表合并为一个[[ HintInfo ]]。
   */
  private def mergeHints(hints: Seq[HintInfo]): Option[HintInfo] = {
    hints.reduceOption((h1, h2) => h1.merge(h2, hintErrorHandler))
  }

  /**
   * 从计划中提取所有提示，返回提取的提示列表和转换后的计划
   * 删除了[[ ResolvedHint ]]节点。返回的提示列表以自上而下的顺序排列。
   * 请注意，提示只能从某些节点下提取。那些无法提取的
   * 此方法稍后将被此规则清理，并可能会发出警告，具体取决于配置。
   */
  private[sql] def extractHintsFromPlan(plan: LogicalPlan): (LogicalPlan, Seq[HintInfo]) = {
    plan match {
      case h: ResolvedHint =>
        val (plan, hints) = extractHintsFromPlan(h.child)
        (plan, h.hints +: hints)
      case u: UnaryNode =>
        val (plan, hints) = extractHintsFromPlan(u.child)
        (u.withNewChildren(Seq(plan)), hints)
      // TODO revisit this logic:
      // 除了和交叉是半连接/反连接，然后不会返回更多数据
      // 他们的左参数，所以广播提示应该在这里传播
      case i: Intersect =>
        val (plan, hints) = extractHintsFromPlan(i.left)
        (i.copy(left = plan), hints)
      case e: Except =>
        val (plan, hints) = extractHintsFromPlan(e.left)
        (e.copy(left = plan), hints)
      case p: LogicalPlan => (p, Seq.empty)
    }
  }
}
