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

import org.apache.spark.sql.catalyst.expressions.{And, ArrayExists, ArrayFilter, CaseWhen, Expression, If}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, Literal, MapFilter, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.Utils


/**
*如果可能，在搜索中用`FalseLiteral`替换`Literal（null，BooleanType）`的规则
 * WHERE / HAVING / ON（JOIN）子句的条件，包含隐式布尔运算符
*“（搜索条件）= TRUE”。替换仅在`Literal（null，BooleanType）`时有效
 *在评估整个搜索条件时，语义上等同于`FalseLiteral`。
 *
 * 请注意，在搜索条件下，大多数情况下FALSE和NULL都不可交换
 * 包含NOT和NULL容错表达式。因此，该规则非常保守和适用
 * 在非常有限的情况下。
 *
 * 例如，`Filter（Literal（null，BooleanType））`等于`Filter（FalseLiteral）`。
 *
 * 另一个包含分支的例子是`Filter（If（cond，FalseLiteral，Literal（null，_）））`;
 * 这可以优化为`过滤（If（cond，FalseLiteral，FalseLiteral））`，最终
 * `过滤器（FalseLiteral）`。
 *
 * 此外，此规则还会转换所有[[ If ]]表达式和分支中的谓词
 * 所有[[ CaseWhen ]]表达式中的条件，即使它们不是搜索条件的一部分。
 *
 * 例如，`Project（If（And（cond，Literal（null）），Literal（1），Literal（2）））`可以简化
 * 进入`Project（Literal（2））`。
 */
object ReplaceNullWithFalseInPredicate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(cond, _) => f.copy(condition = replaceNullWithFalse(cond))
    case j @ Join(_, _, _, Some(cond), _) => j.copy(condition = Some(replaceNullWithFalse(cond)))
    case p: LogicalPlan => p transformExpressions {
      case i @ If(pred, _, _) => i.copy(predicate = replaceNullWithFalse(pred))
      case cw @ CaseWhen(branches, _) =>
        val newBranches = branches.map { case (cond, value) =>
          replaceNullWithFalse(cond) -> value
        }
        cw.copy(branches = newBranches)
      case af @ ArrayFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        af.copy(function = newLambda)
      case ae @ ArrayExists(_, lf @ LambdaFunction(func, _, _))
          if !SQLConf.get.getConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        ae.copy(function = newLambda)
      case mf @ MapFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        mf.copy(function = newLambda)
    }
  }

  /**
   *递归遍历布尔类型表达式以替换
   *`Literal（null，BooleanType）`和`FalseLiteral`，如果可能的话。
   *
   *请注意，`transformExpressionsDown`不能在这里使用，因为我们必须在击中后立即停止
   *表达式不是[[ CaseWhen ]]，[[ If ]]，[[ And ]]，[[ Or ]]或
   *`Literal（null，BooleanType）`。
   */
  private def replaceNullWithFalse(e: Expression): Expression = e match {
    case Literal(null, BooleanType) =>
      FalseLiteral
    case And(left, right) =>
      And(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case Or(left, right) =>
      Or(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case cw: CaseWhen if cw.dataType == BooleanType =>
      val newBranches = cw.branches.map { case (cond, value) =>
        replaceNullWithFalse(cond) -> replaceNullWithFalse(value)
      }
      val newElseValue = cw.elseValue.map(replaceNullWithFalse)
      CaseWhen(newBranches, newElseValue)
    case i @ If(pred, trueVal, falseVal) if i.dataType == BooleanType =>
      If(replaceNullWithFalse(pred), replaceNullWithFalse(trueVal), replaceNullWithFalse(falseVal))
    case e if e.dataType == BooleanType =>
      e
    case e =>
      val message = "Expected a Boolean type expression in replaceNullWithFalse, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`."
      if (Utils.isTesting) {
        throw new IllegalArgumentException(message)
      } else {
        logWarning(message)
        e
      }
  }
}
