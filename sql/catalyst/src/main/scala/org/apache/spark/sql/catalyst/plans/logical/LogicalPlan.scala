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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.LogicalPlanStats
import org.apache.spark.sql.types.StructType


abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with AnalysisHelper
  with LogicalPlanStats
  with QueryPlanConstraints
  with Logging {

  /** 如果此子树具有来自流数据源的数据，则返回true。 */
  def isStreaming: Boolean = children.exists(_.isStreaming)

  override def verboseStringWithSuffix(maxFields: Int): String = {
    super.verboseString(maxFields) + statsCache.map(", " + _.toString).getOrElse("")
  }

  /**
   * 返回此计划可以计算的最大行数。
   *
   * 任何可以推送限制的运算符都应重写此函数 (e.g., Union).
   * 任何可以通过限制的运算符都应重写此函数 (e.g., Project).
   */
  def maxRows: Option[Long] = None

  /**
   * 返回此计划在每个分区上可以计算的最大行数。
   */
  def maxRowsPerPartition: Option[Long] = maxRows

  /**
   * 如果此表达式及其所有子级都已解析为特定架构，则返回true
   * 如果它仍然包含任何未解析的占位符，则返回false。
   * 逻辑计划的实现可以覆盖此 (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * 应该返回`false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * 如果已解析此查询计划的所有子级，则返回true。
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * 在此查询计划中将给定架构解析为具体的[[属性]]引用。
   * 此函数只应在分析的计划上调用，因为它将为未解析的[[Attribute]]s抛出[[AnalysisException]]
   * 
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case _ => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(children.flatMap(_.output))

  private[this] lazy val outputAttributes = AttributeSeq(output)

  /**
   * 
   * 也可以使用此逻辑计划的所有子节点的输入将给定字符串解析为 [[NamedExpression]] 。
   * 属性以以下形式表示为字符串：属性以以下形式表示为字符串：
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)

  /**
   * 根据此逻辑计划的输出，可选地将给定字符串解析为[[NamedExpression]] 。
   * 该属性以以下形式表示为字符串：
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)

  /**
   * 给定一个属性名，将其按点拆分为名称部分, 但是
   * 不要拆分反勾号引用的名称部分,例如
   * `ab.cd`.`efg` 应该拆分为两部分分别为 "ab.cd" 何 "efg".
   * 意思就是反勾引用里面是不需要拆分的
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    outputAttributes.resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * 以递归方式刷新（或使）计划中缓存的任何元数据/数据失效。
   */
  def refresh(): Unit = children.foreach(_.refresh())

  /**
   * 返回此计划生成的输出顺序。
   */
  def outputOrdering: Seq[SortOrder] = Nil

  /**
   * 如果`other`'s 在输出语义上是相同，则返回True, ie.:
   *  - 包含相同数量的`Attribute`s;
   *  - 相同引用;
   *  - 序列也是相等的.
   */
  def sameOutput(other: LogicalPlan): Boolean = {
    val thisOutput = this.output
    val otherOutput = other.output
    thisOutput.length == otherOutput.length && thisOutput.zip(otherOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }
}

/**
 * 没有子节点的逻辑计划节点。
 */
abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet

  /** 能够在分析中生存的叶节点必须定义自己的统计信息。*/
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
 * 具有单个子节点的逻辑计划节点。
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil

  /**
   * 通过将原始约束表达式替换为相应的别名，生成包括一组别名约束在内的所有有效约束
   */
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): Set[Expression] = {
    var allConstraints = child.constraints.asInstanceOf[Set[Expression]]
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>
        allConstraints += EqualNullSafe(a.toAttribute, l)
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints
  }

  override protected lazy val validConstraints: Set[Expression] = child.constraints
}

/**
 * 具有左右子级的逻辑计划节点。
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

abstract class OrderPreservingUnaryNode extends UnaryNode {
  override final def outputOrdering: Seq[SortOrder] = child.outputOrdering
}
