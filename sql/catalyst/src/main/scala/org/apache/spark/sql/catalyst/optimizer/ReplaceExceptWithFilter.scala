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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule


/**
 *如果使用逻辑[[ Except ]]运算符中的一个或两个数据集进行纯转换
* [[ Filter ]]，此规则将用[[ Filter ]]运算符替换逻辑[[ Except ]]运算符
 *翻转正确孩子的过滤条件。
 * {{{
 * SELECT a1，a2 FROM Tab1 WHERE a2 = 12除了SELECT a1，a2 FROM Tab1 WHERE a1 = 5
 * ==> SELECT DISTINCT a1，a2 FROM Tab1 WHERE a2 = 12 AND（a1为空或a1 <> 5）
 *}}}
 *
 * 注意：
 * 在翻转右侧节点的过滤条件之前，我们应该：
 * 1.合并所有[[ 过滤器 ]]。
 * 2.更新左侧节点的属性引用;
 * 3.添加Coalesce（条件，False）（考虑条件中的NULL值）。
 */
object ReplaceExceptWithFilter extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    //根据表达式替换Filter，如果不成功则返回不优化，反之继续优化
    if (!plan.conf.replaceExceptWithFilter) {
      return plan
    }
    //开始转换
    plan.transform {
      //若LogicalPlan是可识别的，则继续
      case e @ Except(left, right, false) if isEligible(left, right) =>
        //合并
        val filterCondition = combineFilters(skipProject(right)).asInstanceOf[Filter].condition
        if (filterCondition.deterministic) {
          transformCondition(left, filterCondition).map { c =>
            //去重
            Distinct(Filter(Not(c), left))
          }.getOrElse {
            e
          }
        } else {
          e
        }
    }
  }

  private def transformCondition(plan: LogicalPlan, condition: Expression): Option[Expression] = {
    val attributeNameMap: Map[String, Attribute] = plan.output.map(x => (x.name, x)).toMap
    if (condition.references.forall(r => attributeNameMap.contains(r.name))) {
      val rewrittenCondition = condition.transform {
        case a: AttributeReference => attributeNameMap(a.name)
      }
      // 当条件为NULL时我们需要考虑为False，否则我们不会返回那些
      // 包含NULL的行，而不是在Except右计划中进行过滤
      Some(Coalesce(Seq(rewrittenCondition, Literal.FalseLiteral)))
    } else {
      None
    }
  }

  // TODO: 将来可以进一步扩展。
  private def isEligible(left: LogicalPlan, right: LogicalPlan): Boolean = (left, right) match {
    case (_, right @ (Project(_, _: Filter) | Filter(_, _))) => verifyConditions(left, right)
    case _ => false
  }

  private def verifyConditions(left: LogicalPlan, right: LogicalPlan): Boolean = {
    val leftProjectList = projectList(left)
    val rightProjectList = projectList(right)

    left.output.size == left.output.map(_.name).distinct.size &&
      left.find(_.expressions.exists(SubqueryExpression.hasSubquery)).isEmpty &&
        right.find(_.expressions.exists(SubqueryExpression.hasSubquery)).isEmpty &&
          Project(leftProjectList, nonFilterChild(skipProject(left))).sameResult(
            Project(rightProjectList, nonFilterChild(skipProject(right))))
  }

  private def projectList(node: LogicalPlan): Seq[NamedExpression] = node match {
    case p: Project => p.projectList
    case x => x.output
  }

  private def skipProject(node: LogicalPlan): LogicalPlan = node match {
    case p: Project => p.child
    case x => x
  }

  private def nonFilterChild(plan: LogicalPlan) = plan.find(!_.isInstanceOf[Filter]).getOrElse {
    throw new IllegalStateException("Leaf node is expected")
  }

  private def combineFilters(plan: LogicalPlan): LogicalPlan = {
    @tailrec
    def iterate(plan: LogicalPlan, acc: LogicalPlan): LogicalPlan = {
      if (acc.fastEquals(plan)) acc else iterate(acc, CombineFilters(acc))
    }
    iterate(plan, CombineFilters(plan))
  }
}
