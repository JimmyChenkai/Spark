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

import java.time.LocalDate

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._


/**
 * 查找所有不可计算的表达式，并用可计算的语义等效表达式替换/重写它们。
 * 目前我们替换了两种表达式：
 * 1) [[RuntimeReplaceable]] 表达式
 * 2) [[UnevaluableAggregate]] 表达式 例如：Every, Some, Any, CountIf
 *这主要用于提供与其他数据库的兼容性。
 * Few examples are:
 *   我们使用这个来支持“nvl”，用“coalesce”替换它。
 *   
 *
 * TODO: 在将来，探索替换聚合函数的选项，类似于RuntimeReplacement的功能。
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    //RuntimeReplaceable 表达式
    case e: RuntimeReplaceable => e.child.
    //ContIf 表达式
    case CountIf(predicate) => Count(new NullIf(predicate, Literal.FalseLiteral))
    //Some 表达式
    case SomeAggarg) => Max(arg)
    //Any 表达式
    case AnyAgg(arg) => Max(arg)
    //Every 表达式
    case EveryAgg(arg) => Min(arg)
  }
}


/**
 * 计算当前日期和时间，以确保在单个查询中返回相同的结果。
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val currentDates = mutable.Map.empty[String, Literal]
    val timeExpr = CurrentTimestamp()
    val timestamp = timeExpr.eval(EmptyRow).asInstanceOf[Long]
    val currentTime = Literal.create(timestamp, timeExpr.dataType)

    plan transformAllExpressions {
      case CurrentDate(Some(timeZoneId)) =>
        currentDates.getOrElseUpdate(timeZoneId, {
          Literal.create(
            LocalDate.now(DateTimeUtils.getZoneId(timeZoneId)),
            DateType)
        })
      case CurrentTimestamp() => currentTime
    }
  }
}


/** 用当前数据库名称替换当前数据库的表达式。 */
case class GetCurrentDatabase(sessionCatalog: SessionCatalog) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case CurrentDatabase() =>
        Literal.create(sessionCatalog.getCurrentDatabase, StringType)
    }
  }
}
