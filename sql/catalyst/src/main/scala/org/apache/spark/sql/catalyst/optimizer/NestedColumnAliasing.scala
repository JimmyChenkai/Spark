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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * 这旨在处理`ColumnPruning`优化器规则中的嵌套列别名模式。
 * 如果项目或其子项引用嵌套字段，而不是所有字段
 * 在使用嵌套属性时，我们可以用别名属性替换它们; 然后是一个项目
 * 将创建嵌套字段的*作为子项的子项的别名。
 */
object NestedColumnAliasing {

  def unapply(plan: LogicalPlan)
    : Option[(Map[ExtractValue, Alias], Map[ExprId, Seq[Alias]])] = plan match {
    case Project(projectList, child)
        if SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      getAliasSubMap(projectList)
    case _ => None
  }

  /**
   * 替换嵌套列以便稍后修剪未使用的嵌套列。
   */
  def replaceToAliases(
      plan: LogicalPlan,
      nestedFieldToAlias: Map[ExtractValue, Alias],
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = plan match {
    case Project(projectList, child) =>
      Project(
        getNewProjectList(projectList, nestedFieldToAlias),
        replaceChildrenWithAliases(child, attrToAliases))
  }

  /**
   * 返回已更换的项目列表。
   */
  private def getNewProjectList(
      projectList: Seq[NamedExpression],
      nestedFieldToAlias: Map[ExtractValue, Alias]): Seq[NamedExpression] = {
    projectList.map(_.transform {
      case f: ExtractValue if nestedFieldToAlias.contains(f) =>
        nestedFieldToAlias(f).toAttribute
    }.asInstanceOf[NamedExpression])
  }

  /**
   * 返回一个计划，将新的孩子替换为别名。
   */
  private def replaceChildrenWithAliases(
      plan: LogicalPlan,
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = {
    plan.withNewChildren(plan.children.map { plan =>
      Project(plan.output.flatMap(a => attrToAliases.getOrElse(a.exprId, Seq(a))), plan)
    })
  }

  /**
   * 对于可以推送项目的那些运算符，返回true。
   */
  private def canProjectPushThrough(plan: LogicalPlan) = plan match {
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Repartition => true
    case _: Sample => true
    case _ => false
  }

  /**
   * 返回作为整体单独访问的根引用，以及`GetStructField`s
   * 或`GetArrayStructField'，它位于其他`ExtractValue或特殊表达式之上。
   * 检查`SelectedField`以查看此处应列出的表达式。
   */
  private def collectRootReferenceAndExtractValue(e: Expression): Seq[Expression] = e match {
    case _: AttributeReference => Seq(e)
    case GetStructField(_: ExtractValue | _: AttributeReference, _, _) => Seq(e)
    case GetArrayStructFields(_: MapValues |
                              _: MapKeys |
                              _: ExtractValue |
                              _: AttributeReference, _, _, _, _) => Seq(e)
    case es if es.children.nonEmpty => es.children.flatMap(collectRootReferenceAndExtractValue)
    case _ => Seq.empty
  }

  /**
   * 返回两个映射以将嵌套字段替换为别名。
   *
   * 1. ExtractValue  - >别名：为每个嵌套字段创建一个新别名。
   * 2.ExprId  - > Seq [Alias]：引用属性有多个别名指向它。
   */
  private def getAliasSubMap(projectList: Seq[NamedExpression])
    : Option[(Map[ExtractValue, Alias], Map[ExprId, Seq[Alias]])] = {
    val (nestedFieldReferences, otherRootReferences) =
      projectList.flatMap(collectRootReferenceAndExtractValue).partition {
        case _: ExtractValue => true
        case _ => false
      }

    val aliasSub = nestedFieldReferences.asInstanceOf[Seq[ExtractValue]]
      .filter(!_.references.subsetOf(AttributeSet(otherRootReferences)))
      .groupBy(_.references.head)
      .flatMap { case (attr, nestedFields: Seq[ExtractValue]) =>
        //每个表达式可以包含多个嵌套字段。
        //请注意，我们保留原始名称以区分大小写的方式传递给镶木地板。
        val nestedFieldToAlias = nestedFields.distinct.map { f =>
          val exprId = NamedExpression.newExprId
          (f, Alias(f, s"_gen_alias_${exprId.id}")(exprId, Seq.empty, None))
        }

        //如果使用了`attr`的所有嵌套字段，我们不需要引入新的别名。
        //默认情况下，ColumnPruning规则已使用`attr`。
        if (nestedFieldToAlias.nonEmpty &&
            nestedFieldToAlias.length < totalFieldNum(attr.dataType)) {
          Some(attr.exprId -> nestedFieldToAlias)
        } else {
          None
        }
      }

    if (aliasSub.isEmpty) {
      None
    } else {
      Some((aliasSub.values.flatten.toMap, aliasSub.map(x => (x._1, x._2.map(_._2)))))
    }
  }

  /**
   * 返回此类型的字段总数。这用作使用嵌套列的阈值修剪。可以低估。
   * 如果引用的数量大于此，则为父级使用引用而不是嵌套字段引用。
   */
  private def totalFieldNum(dataType: DataType): Int = dataType match {
    case _: AtomicType => 1
    case StructType(fields) => fields.map(f => totalFieldNum(f.dataType)).sum
    case ArrayType(elementType, _) => totalFieldNum(elementType)
    case MapType(keyType, valueType, _) => totalFieldNum(keyType) + totalFieldNum(valueType)
    case _ => 1 // UDT and others
  }
}
