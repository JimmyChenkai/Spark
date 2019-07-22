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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * 简化冗余的[[createnamedstructlike]]、[[createArray]]和[[createMap]]表达式。
 */
object SimplifyExtractValueOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // 
    // 此优化无效的一个地方是聚合，其中选择列表表达式是分组表达式的函数：
    //
    // SELECT struct(a,b).a FROM tbl GROUP BY struct(a,b)
    //
    // 不可简化成 SELECT a FROM tbl GROUP BY struct(a,b).
    // 因此，只需跳过对聚合的优化（尽管这遗漏了一些可以进行优化的情况）。
    case a: Aggregate => a
    case p => p.transformExpressionsUp {
      // 删除冗余字段提取。
      case GetStructField(createNamedStructLike: CreateNamedStructLike, ordinal, _) =>
        createNamedStructLike.valExprs(ordinal)

      // 删除冗余阵列索引。
      case GetArrayStructFields(CreateArray(elems), field, ordinal, _, _) =>
        // 不要在整个数组中选择字段，而是从数组的每个成员中选择它。
        // 
        // 以这种方式下推操作可能会打开其他优化机会（即struct（…，x，….x）
        CreateArray(elems.map(GetStructField(_, ordinal, Some(field.name))))

      // 删除冗余映射查找。
      case ga @ GetArrayItem(CreateArray(elems), IntegerLiteral(idx)) =>
       
        // 不创建数组，然后选择一行，而是完全删除数组创建。
        if (idx >= 0 && idx < elems.size) {
          // 有效索引
          elems(idx)
        } else {
          // 越界，模拟运行时行为并返回空值
          Literal(null, ga.dataType)
        }
      case GetMapValue(CreateMap(elems), key) => CaseKeyWhen(key, elems)
    }
  }
}
