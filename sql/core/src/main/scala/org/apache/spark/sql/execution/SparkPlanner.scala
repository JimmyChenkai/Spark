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

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.adaptive.LogicalQueryStageStrategy
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.internal.SQLConf

class SparkPlanner(
    val sparkContext: SparkContext,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods)
  extends SparkStrategies {

  def numPartitions: Int = conf.numShufflePartitions

  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      LogicalQueryStageStrategy ::
      PythonEvals ::
      DataSourceV2Strategy ::
      FileSourceStrategy ::
      DataSourceStrategy(conf) ::
      SpecialLimits ::
      Aggregation ::
      Window ::
      JoinSelection ::
      InMemoryScans ::
      BasicOperators :: Nil)

  /**
   * 给planner添加额外的策略，可以覆盖之前策略.
   * 这些自定义策略是在[[ExperimentalMethods]]之后和常规策略之前。
   */
  def extraPlanningStrategies: Seq[Strategy] = Nil

  override protected def collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)] = {
    plan.collect {
      case placeholder @ PlanLater(logicalPlan) => placeholder -> logicalPlan
    }
  }

  override protected def prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan] = {
    // TODO：当我们改进计划空间探索时，我们需要修剪糟糕的计划
    //        防止组合爆炸
    plans
  }

   /**
   *用于构建使用复杂投影和过滤的表扫描操作符
   *独立的物理运营商。此函数返回给定的扫描运算符与Project和
   *仅在需要时添加过滤节点。例如，Project运算符仅在使用时使用
   *最终所需输出需要评估复杂表达式或列可以
   *在过滤完成后进一步消除。
   *
   *`prunePushedDownFilters`参数用于删除可以优化的过滤器
   *通过过滤器下推优化。
   *
   *过滤和表达式评估所需的属性将传递给
   *提供`scanBuilder`功能，以避免不必要的列实现。
   */
  def pruneFilterProject(
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      prunePushedDownFilters: Seq[Expression] => Seq[Expression],
      scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    //即使唯一的评估是应用别名，我们现在仍然使用投影
    //到一个专栏。由于这是一个无操作，因此可以避免。但是，使用这个
    //使用当前实现进行优化会改变输出模式。
    // TODO：从表达式求值中去掉最终输出模式，这样这个副本就可以了
    //安全避免

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
        filterSet.subsetOf(projectSet)) {
      //当可以使用列修剪来获得正确的投影时
      //当此投影的列足以评估所有过滤条件时，
      //只需进行扫描，然后进行过滤，无需额外项目。
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }
}
