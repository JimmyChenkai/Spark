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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**
 *用于将[[LogicalPlan]]转换为物理计划的抽象类。
 *子类负责指定[[GenericStrategy]]对象的列表
 *每个都可以返回可能的物理计划选项列表。
 *如果给定策略无法规划树中的所有剩余运算符，
 *它可以调用[[GenericStrategy＃planLater planLater]]，它返回一个占位符
 *将[[collectPlaceholders collect]]并填写的对象
 *使用其他可用策略。
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** 可被planner执行策略*/
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // 收集物理计划集合，把iterator扁平化
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
