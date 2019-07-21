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
   * Given a column over a base table access, it returns
   * the leaf node column from which the input column is derived.
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
   * Checks if a column has statistics.
   * The column is assumed to be over a base table access.
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
   * Returns the join predicates between two input plans. It only
   * considers basic comparison operators.
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
   * Checks if a star join is a selective join. A star join is assumed
   * to be selective if there are local predicates on the dimension
   * tables.
   */
  private def isSelectiveStarJoin(
      dimTables: Seq[LogicalPlan],
      conditions: Seq[Expression]): Boolean = dimTables.exists {
    case plan @ PhysicalOperation(_, p, _: LeafNode) =>
      // Checks if any condition applies to the dimension tables.
      // Exclude the IsNotNull predicates until predicate selectivity is available.
      // In most cases, this predicate is artificially introduced by the Optimizer
      // to enforce nullability constraints.
      val localPredicates = conditions.filterNot(_.isInstanceOf[IsNotNull])
        .exists(canEvaluate(_, plan))

      // Checks if there are any predicates pushed down to the base table access.
      val pushedDownPredicates = p.nonEmpty && !p.forall(_.isInstanceOf[IsNotNull])

      localPredicates || pushedDownPredicates
    case _ => false
  }

  /**
   * Helper case class to hold (plan, rowCount) pairs.
   */
  private case class TableAccessCardinality(plan: LogicalPlan, size: Option[BigInt])

  /**
   * Returns the cardinality of a base table access. A base table access represents
   * a LeafNode, or Project or Filter operators above a LeafNode.
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
   * Reorders a star join based on heuristics. It is called from ReorderJoin if CBO is disabled.
   *   1) Finds the star join with the largest fact table.
   *   2) Places the fact table the driving arm of the left-deep tree.
   *     This plan avoids large table access on the inner, and thus favor hash joins.
   *   3) Applies the most selective dimensions early in the plan to reduce the amount of
   *      data flow.
   */
  def reorderStarJoins(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): Seq[(LogicalPlan, InnerLike)] = {
    assert(input.size >= 2)

    val emptyStarJoinPlan = Seq.empty[(LogicalPlan, InnerLike)]

    // Find the eligible star plans. Currently, it only returns
    // the star join with the largest fact table.
    val eligibleJoins = input.collect{ case (plan, Inner) => plan }
    val starPlan = findStarJoins(eligibleJoins, conditions)

    if (starPlan.isEmpty) {
      emptyStarJoinPlan
    } else {
      val (factTable, dimTables) = (starPlan.head, starPlan.tail)

      // Only consider selective joins. This case is detected by observing local predicates
      // on the dimension tables. In a star schema relationship, the join between the fact and the
      // dimension table is a FK-PK join. Heuristically, a selective dimension may reduce
      // the result of a join.
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
