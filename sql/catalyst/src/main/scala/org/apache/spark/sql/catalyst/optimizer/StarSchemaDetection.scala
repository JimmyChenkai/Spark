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
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

/**
 * Encapsulates star-schema detection logic.
 */
object StarSchemaDetection extends PredicateHelper {

  private def conf = SQLConf.get

  /**
   * Star schema consists of one or more fact tables referencing a number of dimension
   * tables. In general, star-schema joins are detected using the following conditions:
   *  1. Informational RI constraints (reliable detection)
   * + Dimension contains a primary key that is being joined to the fact table.
   * + Fact table contains foreign keys referencing multiple dimension tables.
   * 2. Cardinality based heuristics
   * + Usually, the table with the highest cardinality is the fact table.
   * + Table being joined with the most number of tables is the fact table.
   *
   * To detect star joins, the algorithm uses a combination of the above two conditions.
   * The fact table is chosen based on the cardinality heuristics, and the dimension
   * tables are chosen based on the RI constraints. A star join will consist of the largest
   * fact table joined with the dimension tables on their primary keys. To detect that a
   * column is a primary key, the algorithm uses table and column statistics.
   *
   * The algorithm currently returns only the star join with the largest fact table.
   * Choosing the largest fact table on the driving arm to avoid large inners is in
   * general a good heuristic. This restriction will be lifted to observe multiple
   * star joins.
   *
   * The highlights of the algorithm are the following:
   *
   * Given a set of joined tables/plans, the algorithm first verifies if they are eligible
   * for star join detection. An eligible plan is a base table access with valid statistics.
   * A base table access represents Project or Filter operators above a LeafNode. Conservatively,
   * the algorithm only considers base table access as part of a star join since they provide
   * reliable statistics. This restriction can be lifted with the CBO enablement by default.
   *
   * If some of the plans are not base table access, or statistics are not available, the algorithm
   * returns an empty star join plan since, in the absence of statistics, it cannot make
   * good planning decisions. Otherwise, the algorithm finds the table with the largest cardinality
   * (number of rows), which is assumed to be a fact table.
   *
   * Next, it computes the set of dimension tables for the current fact table. A dimension table
   * is assumed to be in a RI relationship with a fact table. To infer column uniqueness,
   * the algorithm compares the number of distinct values with the total number of rows in the
   * table. If their relative difference is within certain limits (i.e. ndvMaxError * 2, adjusted
   * based on 1TB TPC-DS data), the column is assumed to be unique.
   */
  def findStarJoins(
      input: Seq[LogicalPlan],
      conditions: Seq[Expression]): Seq[LogicalPlan] = {

    //定义一个空星型Plan
    val emptyStarJoinPlan = Seq.empty[LogicalPlan]
    //如果size<2 为emptyStarPlan
    if (input.size < 2) {
      emptyStarJoinPlan
    } else {
      // 查找输入计划是否符合星型联接检测。
      // 符合条件的计划是具有有效统计信息的基表访问。
      val foundEligibleJoin = input.forall {
        case PhysicalOperation(_, _, t: LeafNode) if t.stats.rowCount.isDefined => true
        case _ => false
      }

      if (!foundEligibleJoin) {
        // 有些计划没有统计数据或是复杂的计划。保守地说，返回空的星形联接。
        // 这个限制可以解除,在计划中传播统计信息后。
        emptyStarJoinPlan
      } else {
        // 使用基于基数的启发式方法查找事实表，即行数最大的表。
        val sortedFactTables = input.map { plan =>
          TableAccessCardinality(plan, getTableAccessCardinality(plan))
        }.collect { case t @ TableAccessCardinality(_, Some(_)) =>
          t
        }.sortBy(_.size)(implicitly[Ordering[Option[BigInt]]].reverse)

        sortedFactTables match {
          case Nil =>
            emptyStarJoinPlan
          case table1 :: table2 :: _
            if table2.size.get.toDouble > conf.starSchemaFTRatio * table1.size.get.toDouble =>
            // 如果最大的表具有可比较的行数，则返回空的星型图。
            // 此限制将在推广算法时解除,返回星型Plan
            emptyStarJoinPlan
          case TableAccessCardinality(factTable, _) :: rest =>
            // 寻找事实表链接
            val allFactJoins = rest.collect { case TableAccessCardinality(plan, _)
              if findJoinConditions(factTable, plan, conditions).nonEmpty =>
              plan
            }

            // 找到相应的连接条件。
            val allFactJoinCond = allFactJoins.flatMap { plan =>
              val joinCond = findJoinConditions(factTable, plan, conditions)
              joinCond
            }

            // 验证联接列是否具有有效的统计信息。
            // 允许在表之间进行任何关系比较。
            // 稍后，我们将启发式地选择Equi联接表的子集。
            val areStatsAvailable = allFactJoins.forall { dimTable =>
              allFactJoinCond.exists {
                case BinaryComparison(lhs: AttributeReference, rhs: AttributeReference) =>
                  val dimCol = if (dimTable.outputSet.contains(lhs)) lhs else rhs
                  val factCol = if (factTable.outputSet.contains(lhs)) lhs else rhs
                  hasStatistics(dimCol, dimTable) && hasStatistics(factCol, factTable)
                case _ => false
              }
            }

            if (!areStatsAvailable) {
              emptyStarJoinPlan
            } else {
             // 查找维度表的子集。假设维度表位于与事实表的关系。只考虑等价连接在事实和维度表之间，以避免展开联接。
              val eligibleDimPlans = allFactJoins.filter { dimTable =>
                allFactJoinCond.exists {
                  case cond @ Equality(lhs: AttributeReference, rhs: AttributeReference) =>
                    val dimCol = if (dimTable.outputSet.contains(lhs)) lhs else rhs
                    isUnique(dimCol, dimTable)
                  case _ => false
                }
              }

              if (eligibleDimPlans.isEmpty || eligibleDimPlans.size < 2) {
                // 找不到符合条件的星型联接，因为该联接不是RI联接，或者星型联接是扩展联接。
                // 此外，一个星型将涉及多个维度表。
                emptyStarJoinPlan
              } else {
                factTable +: eligibleDimPlans
              }
            }
        }
      }
    }
  }

  /**
   * 确定基表访问所引用的列是否为主键。
   * 如果列不可为空且具有唯一值，则该列为pk。
   * 若要确定某列在没有信息RI约束的情况下是否具有唯一值，
   * 请将不同值的数目与表中的总行数进行比较。
   * 如果它们的相对差在预期范围内(i.e. 2 * spark.sql.statistics.ndv.maxError based
   * on TPC-DS data results), 假定列具有唯一的值。
   */
  private def isUnique(
      column: Attribute,
      plan: LogicalPlan): Boolean = plan match {
    
    case PhysicalOperation(_, _, t: LeafNode) =>
      //找到leafNode列
      val leafCol = findLeafNodeCol(column, plan)
      leafCol match {
        case Some(col) if t.outputSet.contains(col) =>
          val stats = t.stats
          stats.rowCount match {
            //如果列大于0
            case Some(rowCount) if rowCount >= 0 =>
              if (stats.attributeStats.nonEmpty && stats.attributeStats.contains(col)) {
                val colStats = stats.attributeStats.get(col).get
                if (!colStats.hasCountStats || colStats.nullCount.get > 0) {
                  false
                } else {
                  val distinctCount = colStats.distinctCount.get
                  val relDiff = math.abs((distinctCount.toDouble / rowCount.toDouble) - 1.0d)
                  // 基于TPCDS 1TB数据结果调整的ndvmax错误
                  relDiff <= conf.ndvMaxError * 2
                }
              } else {
                false
              }
            case None => false
          }
        case None => false
      }
    case _ => false
  }

  /**
   * 给定基表访问上的列，它将返回派生输入列的叶节点列。
   */
  @tailrec
  private def findLeafNodeCol(
      column: Attribute,
      plan: LogicalPlan): Option[Attribute] = plan match {
    case pl @ PhysicalOperation(_, _, _: LeafNode) =>
      pl match {
        case t: LeafNode if t.outputSet.contains(column) =>
          Option(column)
        case p: Project if p.outputSet.exists(_.semanticEquals(column)) =>
          val col = p.outputSet.find(_.semanticEquals(column)).get
          findLeafNodeCol(col, p.child)
        case f: Filter =>
          findLeafNodeCol(column, f.child)
        case _ => None
      }
    case _ => None
  }

  /**
   * 检查列是否有统计信息。假定列位于基表访问之上。
   */
  private def hasStatistics(
      column: Attribute,
      plan: LogicalPlan): Boolean = plan match {
    case PhysicalOperation(_, _, t: LeafNode) =>
      val leafCol = findLeafNodeCol(column, plan)
      leafCol match {
        case Some(col) if t.outputSet.contains(col) =>
          val stats = t.stats
          stats.attributeStats.nonEmpty && stats.attributeStats.contains(col)
        case None => false
      }
    case _ => false
  }

  /**
   * 返回两个输入计划之间的联接谓词。它只考虑基本的比较运算符。
   */
  @inline
  private def findJoinConditions(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      conditions: Seq[Expression]): Seq[Expression] = {
    val refs = plan1.outputSet ++ plan2.outputSet
    conditions.filter {
      case BinaryComparison(_, _) => true
      case _ => false
    }.filterNot(canEvaluate(_, plan1))
      .filterNot(canEvaluate(_, plan2))
      .filter(_.references.subsetOf(refs))
  }

  /**
   * 检查星形联接是否为选择性联接。
   * 如果维度表上有本地谓词，则假定星型联接是选择性的。
   */
  private def isSelectiveStarJoin(
      dimTables: Seq[LogicalPlan],
      conditions: Seq[Expression]): Boolean = dimTables.exists {
    case plan @ PhysicalOperation(_, p, _: LeafNode) =>
    // 检查是否有任何条件适用于维度表。
    // 排除isnotnull谓词，直到谓词选择性可用。
    // 在大多数情况下，这个谓词是由优化器人工引入的
    // 强制为空性约束。
      val localPredicates = conditions.filterNot(_.isInstanceOf[IsNotNull])
        .exists(canEvaluate(_, plan))

      // 检查是否有任何谓词被下推到基表访问。
      val pushedDownPredicates = p.nonEmpty && !p.forall(_.isInstanceOf[IsNotNull])

      localPredicates || pushedDownPredicates
    case _ => false
  }

  /**
   * 要保留（plan，rowcount）对的helper case类。
   */
  private case class TableAccessCardinality(plan: LogicalPlan, size: Option[BigInt])

  /**
   * 返回基表访问的基数。
   * 基表访问表示leafnode或leafnode上方的项目或筛选器运算符。
   */
  private def getTableAccessCardinality(
      input: LogicalPlan): Option[BigInt] = input match {
    case PhysicalOperation(_, cond, t: LeafNode) if t.stats.rowCount.isDefined =>
      if (conf.cboEnabled && input.stats.rowCount.isDefined) {
        Option(input.stats.rowCount.get)
      } else {
        Option(t.stats.rowCount.get)
      }
    case _ => None
  }

  /**
   * 基于启发式重新排序星形联接。如果禁用cbo，则从reorderjoin调用。
   *   1) 以事实表最大连接数为判断星型链接方式
   *   2) 将事实表放置在左深树的 driving arm上。
   *     这个计划避免了内部的大表访问，因此有利于哈希连接。
   *   3) 在计划的早期应用最有选择的维度，以减少数据流的数量。
   *      
   */
  def reorderStarJoins(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): Seq[(LogicalPlan, InnerLike)] = {
    assert(input.size >= 2)

    val emptyStarJoinPlan = Seq.empty[(LogicalPlan, InnerLike)]

    // 找到符合条件的星型计划。目前，它只返回
    // 星形与最大事实表联接。
    val eligibleJoins = input.collect{ case (plan, Inner) => plan }
    val starPlan = findStarJoins(eligibleJoins, conditions)

    if (starPlan.isEmpty) {
      emptyStarJoinPlan
    } else {
      val (factTable, dimTables) = (starPlan.head, starPlan.tail)

      //只考虑选择性连接。这种情况是通过观察本地谓词来检测的在维度表上
      //在星型模式关系中，事实和维度表是FK-PK联接
      //从启发式的角度来看，选择维度可能会减少联接的结果。
      if (isSelectiveStarJoin(dimTables, conditions)) {
        val reorderDimTables = dimTables.map { plan =>
          TableAccessCardinality(plan, getTableAccessCardinality(plan))
        }.sortBy(_.size).map {
          case TableAccessCardinality(p1, _) => p1
        }

        val reorderStarPlan = factTable +: reorderDimTables
        reorderStarPlan.map(plan => (plan, Inner))
      } else {
        emptyStarJoinPlan
      }
    }
  }
}
