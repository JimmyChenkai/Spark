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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf


/**
 * 基于成本的联接重新排序。我们将来可能会有几种连接重排序算法。这个类是这些算法的入口，并选择要使用的算法。
 * 继承了 Rule[LogicalPlan]
 * 实现apply方法
 */
object CostBasedJoinReorder extends Rule[LogicalPlan] with PredicateHelper {

  private def conf = SQLConf.get

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.cboEnabled || !conf.joinReorderEnabled) {
      plan
    } else {
      val result = plan transformDown {
        // 用一个可连接项开始重新排序，它是一个与条件类似的内部连接。
        // 如果存在连接提示，请避免重新排序。
        case j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE) =>
          reorder(j, j.output)
        case p @ Project(projectList, Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE))
          if projectList.forall(_.isInstanceOf[Attribute]) =>
          reorder(p, p.output)
      }
      // 重新排序完成后，将orderedjoin转换回join。
      result transform {
        case OrderedJoin(left, right, jt, cond) => Join(left, right, jt, cond, JoinHint.NONE)
      }
    }
  }

  private def reorder(plan: LogicalPlan, output: Seq[Attribute]): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    val result =
      // 如果项目数合适并且存在连接条件，则重新排序。
      // 我们还需要检查所有项目的成本是否可以评估。
      if (items.size > 2 && items.size <= conf.joinReorderDPThreshold && conditions.nonEmpty &&
          items.forall(_.stats.rowCount.isDefined)) {
        JoinReorderDP.search(conf, items, conditions, output)
      } else {
        plan
      }
    // 设置连续连接节点的顺序。
    replaceWithOrderedJoin(result)
  }

  /**
   * Extracts items of consecutive inner joins and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Set[Expression]) = {
    plan match {
      case Join(left, right, _: InnerLike, Some(cond), JoinHint.NONE) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, splitConjunctivePredicates(cond).toSet ++
          leftConditions ++ rightConditions)
      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), Set())
    }
  }

  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
    case j @ Join(left, right, jt: InnerLike, Some(cond), JoinHint.NONE) =>
      val replacedLeft = replaceWithOrderedJoin(left)
      val replacedRight = replaceWithOrderedJoin(right)
      OrderedJoin(replacedLeft, replacedRight, jt, Some(cond))
    case p @ Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE)) =>
      p.copy(child = replaceWithOrderedJoin(j))
    case _ =>
      plan
  }
}

/** This is a mimic class for a join node that has been ordered. */
case class OrderedJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}

/**
 * 使用动态编程算法对连接重新排序。此实现基于
 * 论文：关系数据库管理系统中的访问路径选择。
 * http://www.inf.ed.ac.uk/teaching/courses/adbs/AccessPath.pdf
 * 
 * 首先，我们将所有项（基本连接节点）放入级别0，然后构建所有双向连接。
 * 在0级计划的1级（单个项目），然后从计划构建所有3向联接
 * 在前面的级别（双向联接和单个项），然后是四向联接…等等，直到我们
 * 构建所有N向连接，并在其中选择最佳计划。
 *
 *
 * 对于成本评估，由于运营商的实际成本目前不可用，因此我们使用
 * 计算成本的基数和大小。
 */
object JoinReorderDP extends PredicateHelper with Logging {

  def search(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      output: Seq[Attribute]): LogicalPlan = {

    val startTime = System.nanoTime()
    // Level i maintains all found plans for i + 1 items.
    // Create the initial plans: each plan is a single item with zero cost.
    val itemIndex = items.zipWithIndex
    val foundPlans = mutable.Buffer[JoinPlanMap](itemIndex.map {
      case (item, id) => Set(id) -> JoinPlan(Set(id), item, Set.empty, Cost(0, 0))
    }.toMap)

    // Build filters from the join graph to be used by the search algorithm.
    val filters = JoinReorderDPFilters.buildJoinGraphInfo(conf, items, conditions, itemIndex)

    // Build plans for next levels until the last level has only one plan. This plan contains
    // all items that can be joined, so there's no need to continue.
    val topOutputSet = AttributeSet(output)
    while (foundPlans.size < items.length) {
      // Build plans for the next level.
      foundPlans += searchLevel(foundPlans, conf, conditions, topOutputSet, filters)
    }

    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logDebug(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
      s"${items.length}, number of plans in memo: ${foundPlans.map(_.size).sum}")

    // The last level must have one and only one plan, because all items are joinable.
    assert(foundPlans.size == items.length && foundPlans.last.size == 1)
    foundPlans.last.head._2.plan match {
      case p @ Project(projectList, j: Join) if projectList != output =>
        assert(topOutputSet == p.outputSet)
        // Keep the same order of final output attributes.
        p.copy(projectList = output)
      case finalPlan if !sameOutput(finalPlan, output) =>
        Project(output, finalPlan)
      case finalPlan =>
        finalPlan
    }
  }

  private def sameOutput(plan: LogicalPlan, expectedOutput: Seq[Attribute]): Boolean = {
    val thisOutput = plan.output
    thisOutput.length == expectedOutput.length && thisOutput.zip(expectedOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  /** Find all possible plans at the next level, based on existing levels. */
  private def searchLevel(
      existingLevels: Seq[JoinPlanMap],
      conf: SQLConf,
      conditions: Set[Expression],
      topOutput: AttributeSet,
      filters: Option[JoinGraphInfo]): JoinPlanMap = {

    val nextLevel = mutable.Map.empty[Set[Int], JoinPlan]
    var k = 0
    val lev = existingLevels.length - 1
    // Build plans for the next level from plans at level k (one side of the join) and level
    // lev - k (the other side of the join).
    // For the lower level k, we only need to search from 0 to lev - k, because when building
    // a join from A and B, both A J B and B J A are handled.
    while (k <= lev - k) {
      val oneSideCandidates = existingLevels(k).values.toSeq
      for (i <- oneSideCandidates.indices) {
        val oneSidePlan = oneSideCandidates(i)
        val otherSideCandidates = if (k == lev - k) {
          // Both sides of a join are at the same level, no need to repeat for previous ones.
          oneSideCandidates.drop(i)
        } else {
          existingLevels(lev - k).values.toSeq
        }

        otherSideCandidates.foreach { otherSidePlan =>
          buildJoin(oneSidePlan, otherSidePlan, conf, conditions, topOutput, filters) match {
            case Some(newJoinPlan) =>
              // Check if it's the first plan for the item set, or it's a better plan than
              // the existing one due to lower cost.
              val existingPlan = nextLevel.get(newJoinPlan.itemIds)
              if (existingPlan.isEmpty || newJoinPlan.betterThan(existingPlan.get, conf)) {
                nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
              }
            case None =>
          }
        }
      }
      k += 1
    }
    nextLevel.toMap
  }

  /**
   * 建立一个新的Joinplan if the following conditions hold:
   * - 左右两侧包含的项目集不重叠。
   * - 至少存在一个涉及两侧引用的联接条件。
   * - 如果启用了星形联接筛选器，则允许以下组合：
   *         1) (oneJoinPlan U otherJoinPlan)是star-join的子集
   *         2) star-join 是(oneJoinPlan U otherJoinPlan)的子集
   *         3) (oneJoinPlan U otherJoinPlan) 是non star-join的子集
   *
   * @param oneJoinPlan One side JoinPlan for building a new JoinPlan.
   * @param otherJoinPlan The other side JoinPlan for building a new join node.
   * @param conf SQLConf for statistics computation.
   * @param conditions The overall set of join conditions.
   * @param topOutput The output attributes of the final plan.
   * @param filters Join graph info to be used as filters by the search algorithm.
   * @return Builds and returns a new JoinPlan if both conditions hold. Otherwise, returns None.
   */
  private def buildJoin(
      oneJoinPlan: JoinPlan,
      otherJoinPlan: JoinPlan,
      conf: SQLConf,
      conditions: Set[Expression],
      topOutput: AttributeSet,
      filters: Option[JoinGraphInfo]): Option[JoinPlan] = {

    if (oneJoinPlan.itemIds.intersect(otherJoinPlan.itemIds).nonEmpty) {
      // 不应联接两个重叠的项集。
      return None
    }

    if (filters.isDefined) {
      // 应用星型联接筛选器，以确保星型架构关系中的表
      // 计划在一起。星型滤波器将消除星型和非星型之间的连接。
      // 直到建立星形联接为止的表。允许以下组合：
      // 1。（OneJoinPlan或OtherJoinPlan）是星形联接的子集。
      // 2。星形联接是（OneJoinPlan或OtherJoinPlan）的子集
      // 3。（OneJoinPlan或OtherJoinPlan）是非星形联接的子集
      val isValidJoinCombination =
        JoinReorderDPFilters.starJoinFilter(oneJoinPlan.itemIds, otherJoinPlan.itemIds,
          filters.get)
      if (!isValidJoinCombination) return None
    }

    val onePlan = oneJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    val joinConds = conditions
      .filterNot(l => canEvaluate(l, onePlan))
      .filterNot(r => canEvaluate(r, otherPlan))
      .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
    if (joinConds.isEmpty) {
      // 笛卡尔产品非常昂贵，所以我们将其排除在候选计划之外。
      // 这也大大减少了搜索空间。
      return None
    }

    // 把较深的一面放在左边，倾向于建一棵左深的树。
    val (left, right) = if (oneJoinPlan.itemIds.size >= otherJoinPlan.itemIds.size) {
      (onePlan, otherPlan)
    } else {
      (otherPlan, onePlan)
    }
    val newJoin = Join(left, right, Inner, joinConds.reduceOption(And), JoinHint.NONE)
    val collectedJoinConds = joinConds ++ oneJoinPlan.joinConds ++ otherJoinPlan.joinConds
    val remainingConds = conditions -- collectedJoinConds
    val neededAttr = AttributeSet(remainingConds.flatMap(_.references)) ++ topOutput
    val neededFromNewJoin = newJoin.output.filter(neededAttr.contains)
    val newPlan =
      if ((newJoin.outputSet -- neededFromNewJoin).nonEmpty) {
        Project(neededFromNewJoin, newJoin)
      } else {
        newJoin
      }

    val itemIds = oneJoinPlan.itemIds.union(otherJoinPlan.itemIds)
    // 现在，oneplan/otherplan的根节点成为中间联接（如果它是非叶节点item），
    // 因此新联接的成本也应该包括它自己的成本。
    val newPlanCost = oneJoinPlan.planCost + oneJoinPlan.rootCost(conf) +
      otherJoinPlan.planCost + otherJoinPlan.rootCost(conf)
    Some(JoinPlan(itemIds, newPlan, collectedJoinConds, newPlanCost))
  }

  /** Map[set of item ids, join plan for these items] */
  type JoinPlanMap = Map[Set[Int], JoinPlan]

  /**
   * 特定级别中的部分联接顺序。
   *
   * @param itemIds Set of item ids participating in this partial plan.
   * @param plan The plan tree with the lowest cost for these items found so far.
   * @param joinConds Join conditions included in the plan.
   * @param planCost The cost of this plan tree is the sum of costs of all intermediate joins.
   */
  case class JoinPlan(
      itemIds: Set[Int],
      plan: LogicalPlan,
      joinConds: Set[Expression],
      planCost: Cost) {

    /**获取此计划树的根节点的成本。*/
    def rootCost(conf: SQLConf): Cost = {
      if (itemIds.size > 1) {
        val rootStats = plan.stats
        Cost(rootStats.rowCount.get, rootStats.sizeInBytes)
      } else {
        // 如果计划是叶项目，则成本为零。
        Cost(0, 0)
      }
    }

    def betterThan(other: JoinPlan, conf: SQLConf): Boolean = {
      if (other.planCost.card == 0 || other.planCost.size == 0) {
        false
      } else {
        val relativeRows = BigDecimal(this.planCost.card) / BigDecimal(other.planCost.card)
        val relativeSize = BigDecimal(this.planCost.size) / BigDecimal(other.planCost.size)
        relativeRows * conf.joinReorderCardWeight +
          relativeSize * (1 - conf.joinReorderCardWeight) < 1
      }
    }
  }
}

/**
 * 此类定义计划的成本模型。
 * @param card Cardinality (number of rows).
 * @param size Size in bytes.
 */
case class Cost(card: BigInt, size: BigInt) {
  def +(other: Cost): Cost = Cost(this.card + other.card, this.size + other.size)
}

/**
 * Implements optional filters to reduce the search space for join enumeration.
 *
 * 1) Star-join filters: Plan star-joins together since they are assumed
 *    to have an optimal execution based on their RI relationship.
 * 2) Cartesian products: Defer their planning later in the graph to avoid
 *    large intermediate results (expanding joins, in general).
 * 3) Composite inners: Don't generate "bushy tree" plans to avoid materializing
 *   intermediate results.
 *
 * Filters (2) and (3) are not implemented.
 */
object JoinReorderDPFilters extends PredicateHelper {
  /**
   * Builds join graph information to be used by the filtering strategies.
   * Currently, it builds the sets of star/non-star joins.
   * It can be extended with the sets of connected/unconnected joins, which
   * can be used to filter Cartesian products.
   */
  def buildJoinGraphInfo(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      itemIndex: Seq[(LogicalPlan, Int)]): Option[JoinGraphInfo] = {

    if (conf.joinReorderDPStarFilter) {
      // Compute the tables in a star-schema relationship.
      val starJoin = StarSchemaDetection.findStarJoins(items, conditions.toSeq)
      val nonStarJoin = items.filterNot(starJoin.contains(_))

      if (starJoin.nonEmpty && nonStarJoin.nonEmpty) {
        val itemMap = itemIndex.toMap
        Some(JoinGraphInfo(starJoin.map(itemMap).toSet, nonStarJoin.map(itemMap).toSet))
      } else {
        // Nothing interesting to return.
        None
      }
    } else {
      // Star schema filter is not enabled.
      None
    }
  }

  /**
   * Applies the star-join filter that eliminates join combinations among star
   * and non-star tables until the star join is built.
   *
   * Given the oneSideJoinPlan/otherSideJoinPlan, which represent all the plan
   * permutations generated by the DP join enumeration, and the star/non-star plans,
   * the following plan combinations are allowed:
   * 1. (oneSideJoinPlan U otherSideJoinPlan) is a subset of star-join
   * 2. star-join is a subset of (oneSideJoinPlan U otherSideJoinPlan)
   * 3. (oneSideJoinPlan U otherSideJoinPlan) is a subset of non star-join
   *
   * It assumes the sets are disjoint.
   *
   * Example query graph:
   *
   * t1   d1 - t2 - t3
   *  \  /
   *   f1
   *   |
   *   d2
   *
   * star: {d1, f1, d2}
   * non-star: {t2, t1, t3}
   *
   * level 0: (f1 ), (d2 ), (t3 ), (d1 ), (t1 ), (t2 )
   * level 1: {t3 t2 }, {f1 d2 }, {f1 d1 }
   * level 2: {d2 f1 d1 }
   * level 3: {t1 d1 f1 d2 }, {t2 d1 f1 d2 }
   * level 4: {d1 t2 f1 t1 d2 }, {d1 t3 t2 f1 d2 }
   * level 5: {d1 t3 t2 f1 t1 d2 }
   *
   * @param oneSideJoinPlan One side of the join represented as a set of plan ids.
   * @param otherSideJoinPlan The other side of the join represented as a set of plan ids.
   * @param filters Star and non-star plans represented as sets of plan ids
   */
  def starJoinFilter(
      oneSideJoinPlan: Set[Int],
      otherSideJoinPlan: Set[Int],
      filters: JoinGraphInfo) : Boolean = {
    val starJoins = filters.starJoins
    val nonStarJoins = filters.nonStarJoins
    val join = oneSideJoinPlan.union(otherSideJoinPlan)

    // Disjoint sets
    oneSideJoinPlan.intersect(otherSideJoinPlan).isEmpty &&
      // Either star or non-star is empty
      (starJoins.isEmpty || nonStarJoins.isEmpty ||
        // Join is a subset of the star-join
        join.subsetOf(starJoins) ||
        // Star-join is a subset of join
        starJoins.subsetOf(join) ||
        // Join is a subset of non-star
        join.subsetOf(nonStarJoins))
  }
}

/**
 * Helper class that keeps information about the join graph as sets of item/plan ids.
 * It currently stores the star/non-star plans. It can be
 * extended with the set of connected/unconnected plans.
 */
case class JoinGraphInfo (starJoins: Set[Int], nonStarJoins: Set[Int])
