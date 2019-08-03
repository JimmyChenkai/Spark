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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.MemoryPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StructType

/**
  *将逻辑计划转换为零个或多个SparkPlans。此API公开用于实验
 *使用查询规划器，并不是为了在spark版本中保持稳定。开发者
 *编写库应考虑使用中提供的稳定API
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override def setLogicalLink(logicalPlan: LogicalPlan): Unit = {}
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  override def plan(plan: LogicalPlan): Iterator[SparkPlan] = {
    super.plan(plan).map { p =>
      val logicalPlan = plan match {
        case ReturnAnswer(rootPlan) => rootPlan
        case _ => plan
      }
      p.setLogicalLink(logicalPlan)
      p
    }
  }

  /**
   * 特殊Limit操作的执行计划
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
     //如果是rootPlan
      case ReturnAnswer(rootPlan) => rootPlan match {
       //Sort
        case Limit(IntegerLiteral(limit), Sort(order, true, child))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
       //Project
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
       
        case Limit(IntegerLiteral(limit), child) =>
          CollectLimitExec(limit, planLater(child)) :: Nil
        case other => planLater(other) :: Nil
      }
     //如果是非rootPlan
      case Limit(IntegerLiteral(limit), Sort(order, true, child))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
     //project
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on join strategy hints, the availability of
   * equi-join keys and the sizes of joining relations. Below are the existing join strategies,
   * their characteristics and their limitations.
   *
   * - Broadcast hash join (BHJ):
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *     BHJ usually performs faster than the other join algorithms when the broadcast side is
   *     small. However, broadcasting tables is a network-intensive operation and it could cause
   *     OOM or perform badly in some cases, especially when the build/broadcast side is big.
   *
   * - Shuffle hash join:
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *
   * - Shuffle sort merge join (SMJ):
   *     Only supported for equi-joins and the join keys have to be sortable.
   *     Supported for all join types.
   *
   * - Broadcast nested loop join (BNLJ):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports all the join types, but the implementation is optimized for:
   *       1) broadcasting the left side in a right outer join;
   *       2) broadcasting the right side in a left outer, left semi, left anti or existence join;
   *       3) broadcasting either side in an inner-like join.
   *     For other cases, we need to scan the data multiple times, which can be rather slow.
   *
   * - Shuffle-and-replicate nested loop join (a.k.a. cartesian product join):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports only inner like joins.
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * 匹配一个计划，其输出应足够小，以便在广播连接中使用。
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
    }

    /**
     * 匹配一个计划，其单个分区应足够小，以构建哈希表。
     *
     * 注意：这假设分区数是固定的，如果需要额外的工作动态
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     *返回计划a是否比计划b小得多（3X）。
     *
     *构建哈希映射的成本高于排序，我们应该只在表上构建哈希映射
     *比其他人小得多。由于我们没有行数统计，
     *使用此处的字节大小作为估计。
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    private def getBuildSide(
        wantToBuildLeft: Boolean,
        wantToBuildRight: Boolean,
        left: LogicalPlan,
        right: LogicalPlan): Option[BuildSide] = {
      if (wantToBuildLeft && wantToBuildRight) {
        //如果我们想构建它，则返回较小的基础估计的物理大小双方
        Some(getSmallerSide(left, right))
      } else if (wantToBuildLeft) {
        Some(BuildLeft)
      } else if (wantToBuildRight) {
        Some(BuildRight)
      } else {
        None
      }
    }

    // 获取左右LogicalPlan最小那个
    private def getSmallerSide(left: LogicalPlan, right: LogicalPlan) = {
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft
    }
    // 左Broadcast hash join
    private def hintToBroadcastLeft(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(BROADCAST))
    }
    // 右Broadcast hash join
    private def hintToBroadcastRight(hint: JoinHint): Boolean = {
      hint.rightHint.exists(_.strategy.contains(BROADCAST))
    }
    // 左Shuffle hash join
    private def hintToShuffleHashLeft(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_HASH))
    }
    // 右Shuffle hash join
    private def hintToShuffleHashRight(hint: JoinHint): Boolean = {
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_HASH))
    }
    // Shuffle sort merge join 
    private def hintToSortMergeJoin(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE)) ||
        hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
    }
    // Broadcast nested loop join
    private def hintToShuffleReplicateNL(hint: JoinHint): Boolean = {
      hint.leftHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL)) ||
        hint.rightHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL))
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      //如果它是等连接，我们首先按以下顺序查看连接提示：
      //    1.广播提示：如果支持连接类型，则选择广播散列连接。如果双方
      //       有广播提示，选择较小的一面（根据统计数据）进行广播。
      //    2.排序合并提示：如果连接键是可排序的，则选择排序合并连接。
      //    3. shuffle hash提示：如果支持连接类型，我们选择shuffle hash join。如果两者
      //       sides有shuffle hash提示，选择较小的一边（基于stats）作为
      //       构建方。
      //    4. shuffle复制NL提示：如果连接类型是内部的，则选择笛卡尔积。
      //
      //如果没有提示或提示不适用，我们将逐一遵循这些规则：
      //    1.如果一边小到可以广播，则选择广播散列连接，以及连接类型
      //       受支持 如果双方都很小，请选择较小的一侧（根据统计数据）
      //       播出
      //    2.如果一边小到足以构建本地哈希映射，则选择shuffle hash join，并且是
      //       比另一边小得多，而`spark.sql.join.preferSortMergeJoin`是假的。
      //    3.如果连接键是可排序的，请选择排序合并连接。
      //    4.如果连接类型是内部的，请选择笛卡尔积。
      //    5.选择广播嵌套循环连接作为最终解决方案。它可能是OOM，但我们没有
      //       其他选择。
     
      // 抽取出来EquiJoinKeys
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint) =>
        //BHJ创建
        def createBroadcastHashJoin(buildLeft: Boolean, buildRight: Boolean) = {
          val wantToBuildLeft = canBuildLeft(joinType) && buildLeft
          val wantToBuildRight = canBuildRight(joinType) && buildRight
          getBuildSide(wantToBuildLeft, wantToBuildRight, left, right).map { buildSide =>
            Seq(joins.BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              condition,
              planLater(left),
              planLater(right)))
          }
        }
        //Shuffle Hash Join创建
        def createShuffleHashJoin(buildLeft: Boolean, buildRight: Boolean) = {
          val wantToBuildLeft = canBuildLeft(joinType) && buildLeft
          val wantToBuildRight = canBuildRight(joinType) && buildRight
          getBuildSide(wantToBuildLeft, wantToBuildRight, left, right).map { buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              condition,
              planLater(left),
              planLater(right)))
          }
        }
        //Shuffle sort merge join 
        def createSortMergeJoin() = {
          if (RowOrdering.isOrderable(leftKeys)) {
            Some(Seq(joins.SortMergeJoinExec(
              leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right))))
          } else {
            None
          }
        }
        //CartesianProduct
        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike]) {
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
          } else {
            None
          }
        }
       //混合join入口处
       //首先判断是否可Broadcast
       //然后是SheffleHash
       //接着是SortMerge
       //再是CartesianProduct
       //最后是BroadcastNestedLoopJoin
        def createJoinWithoutHint() = {
          createBroadcastHashJoin(canBroadcast(left), canBroadcast(right))
            .orElse {
              if (!conf.preferSortMergeJoin) {
                createShuffleHashJoin(
                  canBuildLocalHashMap(left) && muchSmaller(left, right),
                  canBuildLocalHashMap(right) && muchSmaller(right, left))
              } else {
                None
              }
            }
            .orElse(createSortMergeJoin())
            .orElse(createCartesianProduct())
            .getOrElse {
              // 会导致OOM，内存溢出
              val buildSide = getSmallerSide(left, right)
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), buildSide, joinType, condition))
            }
        }

        createBroadcastHashJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None }
          .orElse(createShuffleHashJoin(hintToShuffleHashLeft(hint), hintToShuffleHashRight(hint)))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())

      //如果它不是equi-join，我们首先按以下顺序查看连接提示：
      //    1.广播提示：选择广播嵌套循环连接。如果双方都有广播
      //       提示，选择较小的一面（基于统计数据）来广播内部和完整的连接，
      //       为左连接选择左侧，为左连接选择右侧。
      //    2. shuffle复制NL提示：如果连接类型是内部的，则选择笛卡尔积。
      //
      //如果没有提示或提示不适用，我们将逐一遵循这些规则：
      //    1.如果一边小到足以广播，则选择广播嵌套循环连接。如果只剩下
      //       side是可广播的，它是左连接，或者只有右侧是可广播的
      //       这是正确的加入，我们跳过这个规则。如果双方都很小，则广播较小
      //       用于内部和完全连接的一侧，广播左侧用于右连接，以及广播
      //       左边连接的右侧
      //    2.如果连接类型是内部的，请选择笛卡尔积。
      //    3.选择广播嵌套循环连接作为最终解决方案。它可能是OOM，但我们没有
      //       其他选择。它播放内部和完整连接的较小的一面，广播
      //       左侧为右连接，右侧为左连接。
     
      case logical.Join(left, right, joinType, condition, hint) =>
        val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
          getSmallerSide(left, right)
        } else {
          //由于性能原因，`BroadcastNestedLoopJoinExec`更喜欢广播左侧if
          //这是一个正确的连接，如果是左连接，则播放右侧。
          // TODO：重新审视它。如果左侧比右侧小得多，那可能会更好
          //即使是左连接，也要广播左侧。
          if (canBuildLeft(joinType)) BuildLeft else BuildRight
        }
        //创建BroadcastNLJoin
        def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
          val maybeBuildSide = if (buildLeft && buildRight) {
            Some(desiredBuildSide)
          } else if (buildLeft) {
            Some(BuildLeft)
          } else if (buildRight) {
            Some(BuildRight)
          } else {
            None
          }

          maybeBuildSide.map { buildSide =>
            Seq(joins.BroadcastNestedLoopJoinExec(
              planLater(left), planLater(right), buildSide, joinType, condition))
          }
        }
       //创建CartesianProduct
        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike]) {
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
          } else {
            None
          }
        }
       //混合入口处
       //首先判断是否BroadcastNLJoin
       //然后再判断是否是CartesianProduct
       //BroadcastNestedLoopJoin
        def createJoinWithoutHint() = {
          createBroadcastNLJoin(canBroadcast(left), canBroadcast(right))
            .orElse(createCartesianProduct())
            .getOrElse {
             //此连接可能非常慢或OOM
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), desiredBuildSide, joinType, condition))
            }
        }

        createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())


      // --- Cases where this strategy does not apply ---------------------------------------------
      case _ => Nil
    }
  }

  /**
   * 用于计划作为a的一部分递增计算的流聚合查询
   * [[ StreamingQuery ]]。目前，此规则已注入计划员
   * 按需，仅在[[ org.apache.spark.sql.execution.streaming.StreamExecution ]]中进行规划时
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        if (aggregateExpressions.exists(PythonUDF.isGroupedAggPandasUDF)) {
          throw new AnalysisException(
            "Streaming aggregation doesn't support group aggregate pandas UDF")
        }

        val stateVersion = conf.getConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION)

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = namedGroupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        aggregate.AggUtils.planStreamingAggregation(
          normalizedGroupingExpressions,
          aggregateExpressions.map(expr => expr.asInstanceOf[AggregateExpression]),
          rewrittenResultExpressions,
          stateVersion,
          planLater(child))

      case _ => Nil
    }
  }

  /**
   * 用于规划流式重复数据删除运算符。
   */
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming global limit operator for streams in append mode.
   * We need to check for either a direct Limit or a Limit wrapped in a ReturnAnswer operator,
   * following the example of the SpecialLimits Strategy above.
   * Streams with limit in Append mode use the stateful StreamingGlobalLimitExec.
   * Streams with limit in Complete mode use the stateless CollectLimitExec operator.
   * Limit is unsupported for streams in Update mode.
   */
  case class StreamingGlobalLimitStrategy(outputMode: OutputMode) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), child)
            if plan.isStreaming && outputMode == InternalOutputModes.Append =>
          StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
        case _ => Nil
      }
      case Limit(IntegerLiteral(limit), child)
          if plan.isStreaming && outputMode == InternalOutputModes.Append =>
        StreamingGlobalLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
      case _ => Nil
    }
  }

  object StreamingJoinStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, _)
          if left.isStreaming && right.isStreaming =>

          new StreamingSymmetricHashJoinExec(
            leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

        case Join(left, right, _, _, _) if left.isStreaming && right.isStreaming =>
          throw new AnalysisException(
            "Stream-stream join without equality predicate is not supported", plan = Some(plan))

        case _ => Nil
      }
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
        val aggregateExpressions = aggExpressions.map(expr =>
          expr.asInstanceOf[AggregateExpression])

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children.toSet).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our `RewriteDistinctAggregates` should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = groupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              normalizedGroupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              normalizedGroupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[PythonUDF]) =>
        val udfExpressions = aggExpressions.map(expr => expr.asInstanceOf[PythonUDF])

        Seq(execution.python.AggregateInPandasExec(
          groupingExpressions,
          udfExpressions,
          resultExpressions,
          planLater(child)))

      case PhysicalAggregation(_, _, _, _) =>
        // If cannot match the two cases above, then it's an error
        throw new AnalysisException(
          "Cannot use a mixture of aggregate function and group aggregate pandas UDF")

      case _ => Nil
    }
  }

  object Window extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalWindow(
        WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        execution.python.WindowInPandasExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case s: StreamingRelationV2 =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, child) =>
        val stateVersion = conf.getConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION)
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, None, stateEnc, stateVersion,
          outputMode, timeout, batchTimestampMs = None, eventTimeWatermark = None, planLater(child))
        execPlan :: Nil
      case _ =>
        Nil
    }
  }

  /**
   * Strategy to convert EvalPython logical operator to physical operator.
   */
  object PythonEvals extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ArrowEvalPython(udfs, output, child, evalType) =>
        ArrowEvalPythonExec(udfs, output, planLater(child), evalType) :: Nil
      case BatchEvalPython(udfs, output, child) =>
        BatchEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }

  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case d: DataWritingCommand => DataWritingCommandExec(d, planLater(d.query)) :: Nil
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(StructType.fromAttributes(output))
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right, false) =>
        throw new IllegalStateException(
          "logical intersect  operator should have been replaced by semi-join in the optimizer")
      case logical.Intersect(left, right, true) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.Except(left, right, false) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")
      case logical.Except(left, right, true) =>
        throw new IllegalStateException(
          "logical except (all) operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.ResolvedHint(child, hints) =>
        throw new IllegalStateException(
          "ResolvedHint operator should have been replaced by join hint in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInRWithArrow(f, p, b, is, ot, key, grouping, child) =>
        execution.FlatMapGroupsInRWithArrowExec(
          f, p, b, is, ot, key, grouping, planLater(child)) :: Nil
      case logical.MapPartitionsInRWithArrow(f, p, b, is, ot, child) =>
        execution.MapPartitionsInRWithArrowExec(
          f, p, b, is, ot, planLater(child)) :: Nil
      case logical.FlatMapGroupsInPandas(grouping, func, output, child) =>
        execution.python.FlatMapGroupsInPandasExec(grouping, func, output, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsWithState(
          f, key, value, grouping, data, output, _, _, _, timeout, child) =>
        execution.MapGroupsExec(
          f, key, value, grouping, data, output, timeout, planLater(child)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left), planLater(right)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchangeExec(RoundRobinPartitioning(numPartitions), planLater(child)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data, _) =>
        LocalTableScanExec(output, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(unionChildren.map(planLater)) :: Nil
      case g @ logical.Generate(generator, _, outer, _, _, child) =>
        execution.GenerateExec(
          generator, g.requiredChildOutput, outer,
          g.qualifiedGeneratorOutput, planLater(child)) :: Nil
      case _: logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case r: logical.RepartitionByExpression =>
        exchange.ShuffleExchangeExec(r.partitioning, planLater(r.child)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case _ => Nil
    }
  }
}
