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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * 所有优化器都需要继承Optimizer类
 * 但是拓展优化类是可以重写覆盖
 */
abstract class Optimizer(sessionCatalog: SessionCatalog)
  extends RuleExecutor[LogicalPlan] {

  // 
  // 在测试模式下检查计划的结构完整性。目前，我们在执行每个规则后检查计划是否：
  // - 仍然解决
  // - 仅在受支持的运算符中承载特殊表达式
  override protected def isPlanIntegral(plan: LogicalPlan): Boolean = {
    !Utils.isTesting || (plan.resolved &&
      plan.find(PlanHelper.specialExpressionsInUnsupportedOperator(_).nonEmpty).isEmpty)
  }

  protected def fixedPoint = FixedPoint(SQLConf.get.optimizerMaxIterations)

  /**
   * Defines the default rule batches in the Optimizer.
   *
   * Implementations of this class should override this method, and [[nonExcludableRules]] if
   * necessary, instead of [[batches]]. The rule batches that eventually run in the Optimizer,
   * i.e., returned by [[batches]], will be (defaultBatches - (excludedRules - nonExcludableRules)).
   */
  def defaultBatches: Seq[Batch] = {
    val operatorOptimizationRuleSet =
      Seq(
        // Operator push down
        PushProjectionThroughUnion,                //列剪裁下推
        ReorderJoin,                               //Join顺序优化
        EliminateOuterJoin,                        //OuterJoin消除
        PushPredicateThroughJoin,                  //谓词下推到Join算子
        PushDownPredicate,                         //谓词下推
        PushDownLeftSemiAntiJoin,                  //左半链接下推
        PushLeftSemiLeftAntiThroughJoin,           //左半链接下推到Join算爱
        LimitPushDown,                             //Limit算子下推 
        ColumnPruning,                             //列剪裁 
        InferFiltersFromConstraints,               //约束条件提取
        // Operator combine
        CollapseRepartition,                       //重分区组合
        CollapseProject,                           //投影算子组合
        CollapseWindow,                            //Window组合
        CombineFilters,                            //过滤条件组合 
        CombineLimits,                             //Limit操作组合 
        CombineUnions,                             //Union操作组合  
        // Constant folding and strength reduction
        TransposeWindow,                           //Window转换
        NullPropagation,                           //Null提取
        ConstantPropagation,                       //常量提取 
        FoldablePropagation,                       //可折叠算子提取 
        OptimizeIn,                                //In操作优化 
        ConstantFolding,                           //常数折叠
        ReorderAssociativeOperator,                //重排序关联算子优化
        LikeSimplification,                        //Like算子简化
        BooleanSimplification,                     //Boolean算子简化 
        SimplifyConditionals,                      //条件简化
        RemoveDispensableExpressions,              //Dispensable表达式消除
        SimplifyBinaryComparison,                  //比较算子简化
        ReplaceNullWithFalseInPredicate,           //In替换Null提取  
        PruneFilters,                              //过滤条件剪裁 
        EliminateSorts,                            //排序算子消除
        SimplifyCasts,                             //Cast算子简化
        SimplifyCaseConversionExpressions,         //Case表达式简化
        RewriteCorrelatedScalarSubquery,           //依赖子查询重写 
        EliminateSerialization,                    //序列化消除 
        RemoveRedundantAliases,                    //冗余别名消除
        RemoveNoopOperators,                       //无效操作消除 
        SimplifyExtractValueOps,                   //提取操作简化 
        CombineConcats) ++                         //常量拼接优化
        extendedOperatorOptimizationRules

    val operatorOptimizationBatch: Seq[Batch] = {
      val rulesWithoutInferFiltersFromConstraints =
        operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
      Batch("Operator Optimization before Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) ::
      Batch("Infer Filters", Once,
        InferFiltersFromConstraints) ::
      Batch("Operator Optimization after Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) :: Nil
    }

    (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateResolvedHint,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::
    Batch("OptimizeLimitZero", Once,
      OptimizeLimitZero) ::
    // Run this once earlier. This might simplify the plan and reduce cost of optimizer.
    // For example, a query such as Filter(LocalRelation) would go through all the heavy
    // optimizer rules that are triggered when there is a filter
    // (e.g. InferFiltersFromConstraints). If we run this batch earlier, the query becomes just
    // LocalRelation and does not trigger many rules.
    Batch("LocalRelation early", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) ::
    Batch("Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      RewriteExceptAll,
      RewriteIntersectAll,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil ++
    operatorOptimizationBatch) :+
    Batch("Join Reorder", Once,
      CostBasedJoinReorder) :+
    Batch("Remove Redundant Sorts", Once,
      RemoveRedundantSorts) :+
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) :+
    Batch("Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters,
      ObjectSerializerPruning) :+
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) :+
    Batch("Extract PythonUDF From JoinCondition", Once,
      PullOutPythonUDFInJoinCondition) :+
    // The following batch should be executed after batch "Join Reorder" "LocalRelation" and
    // "Extract PythonUDF From JoinCondition".
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts) :+
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      ColumnPruning,
      CollapseProject,
      RemoveNoopOperators) :+
    // This batch must be executed after the `RewriteSubquery` batch, which creates joins.
    Batch("NormalizeFloatingNumbers", Once, NormalizeFloatingNumbers)
  }

  /**
   * Defines rules that cannot be excluded from the Optimizer even if they are specified in
   * SQL config "excludedRules".
   *
   * Implementations of this class can override this method if necessary. The rule batches
   * that eventually run in the Optimizer, i.e., returned by [[batches]], will be
   * (defaultBatches - (excludedRules - nonExcludableRules)).
   */
  def nonExcludableRules: Seq[String] =
    EliminateDistinct.ruleName ::
      EliminateResolvedHint.ruleName ::
      EliminateSubqueryAliases.ruleName ::
      EliminateView.ruleName ::
      ReplaceExpressions.ruleName ::
      ComputeCurrentTime.ruleName ::
      GetCurrentDatabase(sessionCatalog).ruleName ::
      RewriteDistinctAggregates.ruleName ::
      ReplaceDeduplicateWithAggregate.ruleName ::
      ReplaceIntersectWithSemiJoin.ruleName ::
      ReplaceExceptWithFilter.ruleName ::
      ReplaceExceptWithAntiJoin.ruleName ::
      RewriteExceptAll.ruleName ::
      RewriteIntersectAll.ruleName ::
      ReplaceDistinctWithAggregate.ruleName ::
      PullupCorrelatedPredicates.ruleName ::
      RewriteCorrelatedScalarSubquery.ruleName ::
      RewritePredicateSubquery.ruleName ::
      PullOutPythonUDFInJoinCondition.ruleName ::
      NormalizeFloatingNumbers.ruleName :: Nil

  /**
   * Optimize all the subqueries inside expression.
   */
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case Sort(_, _, child) => child
        case Project(fields, child) => Project(fields, removeTopLevelSort(child))
        case other => other
      }
    }
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        val Subquery(newPlan) = Optimizer.this.execute(Subquery(s.plan))
        // At this point we have an optimized subquery plan that we are going to attach
        // to this subquery expression. Here we can safely remove any top level sort
        // in the plan as tuples produced by a subquery are un-ordered.
        s.withNewPlan(removeTopLevelSort(newPlan))
    }
  }

  /**
   * Override to provide additional rules for the operator optimization batch.
   */
  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Returns (defaultBatches - (excludedRules - nonExcludableRules)), the rule batches that
   * eventually run in the Optimizer.
   *
   * Implementations of this class should override [[defaultBatches]], and [[nonExcludableRules]]
   * if necessary, instead of this method.
   */
  final override def batches: Seq[Batch] = {
    val excludedRulesConf =
      SQLConf.get.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
    val excludedRules = excludedRulesConf.filter { ruleName =>
      val nonExcludable = nonExcludableRules.contains(ruleName)
      if (nonExcludable) {
        logWarning(s"Optimization rule '${ruleName}' was not excluded from the optimizer " +
          s"because this rule is a non-excludable rule.")
      }
      !nonExcludable
    }
    if (excludedRules.isEmpty) {
      defaultBatches
    } else {
      defaultBatches.flatMap { batch =>
        val filteredRules = batch.rules.filter { rule =>
          val exclude = excludedRules.contains(rule.ruleName)
          if (exclude) {
            logInfo(s"Optimization rule '${rule.ruleName}' is excluded from the optimizer.")
          }
          !exclude
        }
        if (batch.rules == filteredRules) {
          Some(batch)
        } else if (filteredRules.nonEmpty) {
          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
        } else {
          logInfo(s"Optimization batch '${batch.name}' is excluded from the optimizer " +
            s"as all enclosed rules have been excluded.")
          None
        }
      }
    }
  }
}

/**
 * Remove useless DISTINCT for MAX and MIN.
 * This rule should be applied before RewriteDistinctAggregates.
 */
object EliminateDistinct extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions  {
    case ae: AggregateExpression if ae.isDistinct =>
      ae.aggregateFunction match {
        case _: Max | _: Min => ae.copy(isDistinct = false)
        case _ => ae
      }
  }
}

/**
 * An optimizer used in test code.
 *
 * To ensure extendability, we leave the standard rules in the abstract optimizer rules, while
 * specific rules go to the subclasses
 */
object SimpleTestOptimizer extends SimpleTestOptimizer

class SimpleTestOptimizer extends Optimizer(
  new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)))

/**
 * Remove redundant aliases from a query plan. A redundant alias is an alias that does not change
 * the name or metadata of a column, and does not deduplicate it.
 */
object RemoveRedundantAliases extends Rule[LogicalPlan] {

  /**
   * Create an attribute mapping from the old to the new attributes. This function will only
   * return the attribute pairs that have changed.
   */
  private def createAttributeMapping(current: LogicalPlan, next: LogicalPlan)
      : Seq[(Attribute, Attribute)] = {
    current.output.zip(next.output).filterNot {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  /**
   * Remove the top-level alias from an expression when it is redundant.
   */
  private def removeRedundantAlias(e: Expression, blacklist: AttributeSet): Expression = e match {
    // Alias with metadata can not be stripped, or the metadata will be lost.
    // If the alias name is different from attribute name, we can't strip it either, or we
    // may accidentally change the output schema name of the root plan.
    case a @ Alias(attr: Attribute, name)
      if a.metadata == Metadata.empty &&
        name == attr.name &&
        !blacklist.contains(attr) &&
        !blacklist.contains(a) =>
      attr
    case a => a
  }

  /**
   * Remove redundant alias expression from a LogicalPlan and its subtree. A blacklist is used to
   * prevent the removal of seemingly redundant aliases used to deduplicate the input for a (self)
   * join or to prevent the removal of top-level subquery attributes.
   */
  private def removeRedundantAliases(plan: LogicalPlan, blacklist: AttributeSet): LogicalPlan = {
    plan match {
      // We want to keep the same output attributes for subqueries. This means we cannot remove
      // the aliases that produce these attributes
      case Subquery(child) =>
        Subquery(removeRedundantAliases(child, blacklist ++ child.outputSet))

      // A join has to be treated differently, because the left and the right side of the join are
      // not allowed to use the same attributes. We use a blacklist to prevent us from creating a
      // situation in which this happens; the rule will only remove an alias if its child
      // attribute is not on the black list.
      case Join(left, right, joinType, condition, hint) =>
        val newLeft = removeRedundantAliases(left, blacklist ++ right.outputSet)
        val newRight = removeRedundantAliases(right, blacklist ++ newLeft.outputSet)
        val mapping = AttributeMap(
          createAttributeMapping(left, newLeft) ++
          createAttributeMapping(right, newRight))
        val newCondition = condition.map(_.transform {
          case a: Attribute => mapping.getOrElse(a, a)
        })
        Join(newLeft, newRight, joinType, newCondition, hint)

      case _ =>
        // Remove redundant aliases in the subtree(s).
        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
        val newNode = plan.mapChildren { child =>
          val newChild = removeRedundantAliases(child, blacklist)
          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
          newChild
        }

        // Create the attribute mapping. Note that the currentNextAttrPairs can contain duplicate
        // keys in case of Union (this is caused by the PushProjectionThroughUnion rule); in this
        // case we use the the first mapping (which should be provided by the first child).
        val mapping = AttributeMap(currentNextAttrPairs)

        // Create a an expression cleaning function for nodes that can actually produce redundant
        // aliases, use identity otherwise.
        val clean: Expression => Expression = plan match {
          case _: Project => removeRedundantAlias(_, blacklist)
          case _: Aggregate => removeRedundantAlias(_, blacklist)
          case _: Window => removeRedundantAlias(_, blacklist)
          case _ => identity[Expression]
        }

        // Transform the expressions.
        newNode.mapExpressions { expr =>
          clean(expr.transform {
            case a: Attribute => mapping.getOrElse(a, a)
          })
        }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
}

/**
 * Remove no-op operators from the query plan that do not make any modifications.
 */
object RemoveNoopOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Eliminate no-op Projects
    case p @ Project(_, child) if child.sameOutput(p) => child

    // Eliminate no-op Window
    case w: Window if w.windowExpressions.isEmpty => w.child
  }
}

/**
 * 下推 LocalLimit 到 UNION 或 Left/Right Outer JOIN之下:
 */
object LimitPushDown extends Rule[LogicalPlan] {

  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case GlobalLimit(_, child) => child
      case _ => plan
    }
  }

  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRowsPerPartition) match {
      case (IntegerLiteral(newLimit), Some(childMaxRows)) if newLimit < childMaxRows =>
        //如果子级在每个分区的max行上有一个cap，并且cap大于新的限制，那么在那里放置一个新的locallimit。
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case (_, None) =>
        // 如果孩子没有上限，则设置新的locallimit。
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case _ =>
        //否则不设置新的locallimit
        plan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //对于Union：
    //若 Union 的任一一边 child 不是一个 limit（GlobalLimit 或 LocalLimit）
    //或是一个 limit 但 limit value 大于 Union parent 的 limit value，
    //以一个 LocalLimit （limit value 为 Union parent limit value）作为 child 的 parent 来作为该边新的 child
    case LocalLimit(exp, Union(children)) =>
      LocalLimit(exp, Union(children.map(maybePushLocalLimit(exp, _))))
    //对于 Outer Join：
    //对于 Left Outer Join，若 left side 不是一个 limit（GlobalLimit 或 LocalLimit）
    //或是一个 limit 但 limit value 大于 Join parent 的 limit value，
    //以一个 LocalLimit（limit value 为 Join parent limit value） 作为 left side 的 parent 来作为新的 left side；
    //对于 Right Outer Join 同理，只是方向不同
    case LocalLimit(exp, join @ Join(left, right, joinType, _, _)) =>
      val newJoin = joinType match {
        case RightOuter => join.copy(right = maybePushLocalLimit(exp, right))
        case LeftOuter => join.copy(left = maybePushLocalLimit(exp, left))
        case _ => join
      }
      LocalLimit(exp, newJoin)
  }
}

/**
 * 将项目运算符推送到联合运算符的两侧。
 * 可安全下推的操作如下所示。
 * Union:
 * 现在，union意味着union all，它不会消除重复的行。 
 * 因此，通过它向下推过滤器和投影是安全的。 过滤器下推由另一个规则下推谓词处理。
 * 一旦我们添加了union distinct，我们就无法向下推预测。
 */
object PushProjectionThroughUnion extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * 将属性从左侧映射到右侧的相应属性。
   */
  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /**
   * 重写表达式，以便将其推送到union或except运算符的右侧。
   * 此方法依赖这样一个事实：
   * union/intersect/except的输出属性始终等于左子级的输出。
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    } match {
      // 确保exprid在union的每个子级中都是唯一的。
      case Alias(child, alias) => Alias(child, alias)()
      case other => other
    }

    // 我们必须向编译器保证，在项目表达式的情况下，不会丢弃名称。
    // 这是安全的，因为唯一的转换是from attribute=>attribute。
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // 通过union all向下推确定性投影
    case p @ Project(projectList, Union(children)) =>
      assert(children.nonEmpty)
      if (projectList.forall(_.deterministic)) {
        val newFirstChild = Project(projectList, children.head)
        val newOtherChildren = children.tail.map { child =>
          val rewrites = buildRewrites(children.head, child)
          Project(projectList.map(pushToRight(_, rewrites)), child)
        }
        Union(newFirstChild +: newOtherChildren)
      } else {
        p
      }
  }
}

/**
 * 尝试从查询计划中消除不需要的列的读取。
 *
 * 由于在筛选器之前添加项目与pushPredicatesThroughProject冲突，此规则将
 * 按以下模式删除项目p2：
 *
 *   p1 @ Project(_, Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(p2.inputSet)
 *
 * p2通常是由这个规则插入的，没有任何用处，p1仍然可以修剪列。
 */
object ColumnPruning extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
    //从项目/聚合/展开的项目列表中删除未使用的列
    case p @ Project(_, p2: Project) if !p2.outputSet.subsetOf(p.references) =>
      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
    //聚合
    case p @ Project(_, a: Aggregate) if !a.outputSet.subsetOf(p.references) =>
      p.copy(
        child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
    //展开项目
    case a @ Project(_, e @ Expand(_, _, grandChild)) if !e.outputSet.subsetOf(a.references) =>
      val newOutput = e.output.filter(a.references.contains(_))
      val newProjects = e.projections.map { proj =>
        proj.zip(e.output).filter { case (_, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, grandChild))

    //从'deserializetoobject'的子级中删除未使用的列`
    case d @ DeserializeToObject(_, _, child) if !child.outputSet.subsetOf(d.references) =>
      d.copy(child = prunedChild(child, d.references))

    // 从aggregate/expand/generate/scriptTransformation的子级中删除未使用的列
    case a @ Aggregate(_, _, child) if !child.outputSet.subsetOf(a.references) =>
      a.copy(child = prunedChild(child, a.references))
    case f @ FlatMapGroupsInPandas(_, _, _, child) if !child.outputSet.subsetOf(f.references) =>
      f.copy(child = prunedChild(child, f.references))
    case e @ Expand(_, _, child) if !child.outputSet.subsetOf(e.references) =>
      e.copy(child = prunedChild(child, e.references))
    case s @ ScriptTransformation(_, _, _, child, _)
        if !child.outputSet.subsetOf(s.references) =>
      s.copy(child = prunedChild(child, s.references))

    // 删除不需要的引用
    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
      val newChild = prunedChild(g.child, requiredAttrs)
      val unrequired = g.generator.references -- p.references
      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1))
        .map(_._2)
      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))

    // 从左侧存在联接的右侧删除不需要的属性。
    case j @ Join(_, right, LeftExistence(_), _, _) =>
      j.copy(right = prunedChild(right, j.references))

    // 所有列都将用于比较，因此我们不能删减它们
    case p @ Project(_, _: SetOperation) => p
    case p @ Project(_, _: Distinct) => p
    // 从联合子项中删除不需要的属性。
    case p @ Project(_, u: Union) =>
      if (!u.outputSet.subsetOf(p.references)) {
        val firstChild = u.children.head
        val newOutput = prunedChild(firstChild, p.references).output
        // 基于已修剪的第一个子级修剪所有子级的列。
        val newChildren = u.children.map { p =>
          val selected = p.output.zipWithIndex.filter { case (a, i) =>
            newOutput.contains(firstChild.output(i))
          }.map(_._1)
          Project(selected, p)
        }
        p.copy(child = u.withNewChildren(newChildren))
      } else {
        p
      }

    // 删除不必要的窗口表达式
    case p @ Project(_, w: Window) if !w.windowOutputSet.subsetOf(p.references) =>
      p.copy(child = w.copy(
        windowExpressions = w.windowExpressions.filter(p.references.contains)))

    // 无法删除leafnode上的列
    case p @ Project(_, _: LeafNode) => p

    case p @ NestedColumnAliasing(nestedFieldToAlias, attrToAliases) =>
      NestedColumnAliasing.replaceToAliases(p, nestedFieldToAlias, attrToAliases)

    // 对于继承它的子项目over project输出的所有其他逻辑计划，都是由第一种情况处理的，请跳过这里。
    case p @ Project(_, child) if !child.isInstanceOf[Project] =>
      val required = child.references ++ p.references
      if (!child.inputSet.subsetOf(required)) {
        val newChildren = child.children.map(c => prunedChild(c, required))
        p.copy(child = child.withNewChildren(newChildren))
      } else {
        p
      }
  })

  /** 仅当子项产生不必要的属性时应用投影 */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if (!c.outputSet.subsetOf(allReferences)) {
      Project(c.output.filter(allReferences.contains), c)
    } else {
      c
    }

  /**
   * 筛选前的项目不是必需的，但与pushPredicatesThroughProject冲突，
   * 所以移除它。由于项目是自上而下添加的，因此需要从下向上移除
   * 订单，否则较低的项目可能会丢失。
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child)))
      if p2.outputSet.subsetOf(child.outputSet) =>
      p1.copy(child = f.copy(child = child))
  }
}

/**
 * Combines two [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression for the following cases.
 * 1. When two [[Project]] operators are adjacent.
 * 2. When two [[Project]] operators have LocalLimit/Sample/Repartition operator between them
 *    and the upper project consists of the same number of columns which is equal or aliasing.
 *    `GlobalLimit(LocalLimit)` pattern is also considered.
 */
object CollapseProject extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, p2: Project) =>
      if (haveCommonNonDeterministicOutput(p1.projectList, p2.projectList)) {
        p1
      } else {
        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
      }
    case p @ Project(_, agg: Aggregate) =>
      if (haveCommonNonDeterministicOutput(p.projectList, agg.aggregateExpressions)) {
        p
      } else {
        agg.copy(aggregateExpressions = buildCleanedProjectList(
          p.projectList, agg.aggregateExpressions))
      }
    case Project(l1, g @ GlobalLimit(_, limit @ LocalLimit(_, p2 @ Project(l2, _))))
        if isRenaming(l1, l2) =>
      val newProjectList = buildCleanedProjectList(l1, l2)
      g.copy(child = limit.copy(child = p2.copy(projectList = newProjectList)))
    case Project(l1, limit @ LocalLimit(_, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
      val newProjectList = buildCleanedProjectList(l1, l2)
      limit.copy(child = p2.copy(projectList = newProjectList))
    case Project(l1, r @ Repartition(_, _, p @ Project(l2, _))) if isRenaming(l1, l2) =>
      r.copy(child = p.copy(projectList = buildCleanedProjectList(l1, p.projectList)))
    case Project(l1, s @ Sample(_, _, _, _, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
      s.copy(child = p2.copy(projectList = buildCleanedProjectList(l1, p2.projectList)))
  }

  private def collectAliases(projectList: Seq[NamedExpression]): AttributeMap[Alias] = {
    AttributeMap(projectList.collect {
      case a: Alias => a.toAttribute -> a
    })
  }

  private def haveCommonNonDeterministicOutput(
      upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Boolean = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Collapse upper and lower Projects if and only if their overlapped expressions are all
    // deterministic.
    upper.exists(_.collect {
      case a: Attribute if aliases.contains(a) => aliases(a).child
    }.exists(!_.deterministic))
  }

  private def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Substitute any attributes that are produced by the lower projection, so that we safely
    // eliminate it.
    // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
    // Use transformUp to prevent infinite recursion.
    val rewrittenUpper = upper.map(_.transformUp {
      case a: Attribute => aliases.getOrElse(a, a)
    })
    // collapse upper and lower Projects may introduce unnecessary Aliases, trim them here.
    rewrittenUpper.map { p =>
      CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
    }
  }

  private def isRenaming(list1: Seq[NamedExpression], list2: Seq[NamedExpression]): Boolean = {
    list1.length == list2.length && list1.zip(list2).forall {
      case (e1, e2) if e1.semanticEquals(e2) => true
      case (Alias(a: Attribute, _), b) if a.metadata == Metadata.empty && a.name == b.name => true
      case _ => false
    }
  }
}

/**
 * 组合相邻的[[RepartitionOperation]]运算符。
 * 重分区组合
 */
object CollapseRepartition extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Case 1: 当重分区具有重分区或重分区ByExpression的子级时，
    // 1) 当top节点不启用shuffle但子节点（即coalescapi）启用shuffle时。如果最后一个numpartitions较大，则返回子节点；否则，保持不变。
    // 2) 在其他情况下，返回具有子节点的顶级节点
    case r @ Repartition(_, _, child: RepartitionOperation) => (r.shuffle, child.shuffle) match {
      case (false, true) => if (r.numPartitions >= child.numPartitions) child else r
      case _ => r.copy(child = child.child)
    }
    // Case 2: 当repartitionbyexpression具有repartition或repartitionbyexpression的子级时
    // 我们可以把级删掉。
    case r @ RepartitionByExpression(_, child: RepartitionOperation, _) =>
      r.copy(child = child.child)
  }
}

/**
 * Collapse Adjacent Window Expression.
 * - If the partition specs and order specs are the same and the window expression are
 *   independent and are of the same window function type, collapse into the parent.
 */
object CollapseWindow extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild))
        if ps1 == ps2 && os1 == os2 && w1.references.intersect(w2.windowOutputSet).isEmpty &&
          we1.nonEmpty && we2.nonEmpty &&
          // This assumes Window contains the same type of window expressions. This is ensured
          // by ExtractWindowFunctions.
          WindowFunctionType.functionType(we1.head) == WindowFunctionType.functionType(we2.head) =>
      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
  }
}

/**
 * Transpose Adjacent Window Expressions.
 * - If the partition spec of the parent Window expression is compatible with the partition spec
 *   of the child window expression, transpose them.
 */
object TransposeWindow extends Rule[LogicalPlan] {
  private def compatibleParititions(ps1 : Seq[Expression], ps2: Seq[Expression]): Boolean = {
    ps1.length < ps2.length && ps2.take(ps1.length).permutations.exists(ps1.zip(_).forall {
      case (l, r) => l.semanticEquals(r)
    })
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild))
        if w1.references.intersect(w2.windowOutputSet).isEmpty &&
           w1.expressions.forall(_.deterministic) &&
           w2.expressions.forall(_.deterministic) &&
           compatibleParititions(ps1, ps2) =>
      Project(w1.output, Window(we2, ps2, os2, Window(we1, ps1, os1, grandChild)))
  }
}

/**
 * 从运算符的现有约束生成附加筛选器列表，
 * 但删除那些已经是运算符条件的一部分或是运算符子约束的一部分的筛选器。
 * 这些筛选器当前插入到筛选器运算符和联接运算符任一侧的现有条件中。
 *
 * Note: 虽然这种优化适用于许多类型的连接，但它主要有利于内部连接和左半连接。
 */
object InferFiltersFromConstraints extends Rule[LogicalPlan]
  with PredicateHelper with ConstraintHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.constraintPropagationEnabled) {
      inferFilters(plan)
    } else {
      plan
    }
  }

  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transform {
    //对于Filter谓词
    case filter @ Filter(condition, child) =>
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }
    //各种链接
    case join @ Join(left, right, joinType, conditionOpt, _) =>
      joinType match {
        // 对于内部连接，我们可以推断出两侧的附加过滤器。LeftSemi是一种内部联接，它只是在最终输出中删除右侧。
        case _: InnerLike | LeftSemi =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(left = newLeft, right = newRight)

        // 对于右侧外部连接，我们只能推断左侧的附加过滤器。
        case RightOuter =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          join.copy(left = newLeft)

        // 对于左连接，我们只能推断右侧的附加过滤器。
        case LeftOuter | LeftAnti =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(right = newRight)

        case _ => join
      }
  }

  // 获取所有约束
  private def getAllConstraints(
      left: LogicalPlan,
      right: LogicalPlan,
      conditionOpt: Option[Expression]): Set[Expression] = {
    val baseConstraints = left.constraints.union(right.constraints)
      .union(conditionOpt.map(splitConjunctivePredicates).getOrElse(Nil).toSet)
    baseConstraints.union(inferAdditionalConstraints(baseConstraints))
  }

  // 推断一个新的Filter
  private def inferNewFilter(plan: LogicalPlan, constraints: Set[Expression]): LogicalPlan = {
    val newPredicates = constraints
      .union(constructIsNotNullConstraints(constraints, plan.output))
      .filter { c =>
        c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
      } -- plan.constraints
    if (newPredicates.isEmpty) {
      plan
    } else {
      Filter(newPredicates.reduce(And), plan)
    }
  }
}

/**
 * Combines all adjacent [[Union]] operators into a single [[Union]].
 */
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case u: Union => flattenUnion(u, false)
    case Distinct(u: Union) => Distinct(flattenUnion(u, true))
  }

  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
    val stack = mutable.Stack[LogicalPlan](union)
    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Distinct(Union(children)) if flattenDistinct =>
          stack.pushAll(children.reverse)
        case Union(children) =>
          stack.pushAll(children.reverse)
        case child =>
          flattened += child
      }
    }
    Union(flattened)
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the non-redundant conditions into
 * one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // The query execution/optimization does not guarantee the expressions are evaluated in order.
    // We only can combine them if and only if both are deterministic.
    case Filter(fc, nf @ Filter(nc, grandChild)) if fc.deterministic && nc.deterministic =>
      (ExpressionSet(splitConjunctivePredicates(fc)) --
        ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
        case Some(ac) =>
          Filter(And(nc, ac), grandChild)
        case None =>
          nf
      }
  }
}

/**
 * Removes no-op SortOrder from Sort
 */
object EliminateSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
      val newOrders = orders.filterNot(_.child.foldable)
      if (newOrders.isEmpty) child else s.copy(order = newOrders)
  }
}

/**
 * Removes redundant Sort operation. This can happen:
 * 1) if the child is already sorted
 * 2) if there is another Sort operator separated by 0...n Project/Filter operators
 */
object RemoveRedundantSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case Sort(orders, true, child) if SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
      child
    case s @ Sort(_, _, child) => s.copy(child = recursiveRemoveSort(child))
  }

  def recursiveRemoveSort(plan: LogicalPlan): LogicalPlan = plan match {
    case Sort(_, _, child) => recursiveRemoveSort(child)
    case other if canEliminateSort(other) =>
      other.withNewChildren(other.children.map(recursiveRemoveSort))
    case _ => plan
  }

  def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
    case p: Project => p.projectList.forall(_.deterministic)
    case f: Filter => f.condition.deterministic
    case _ => false
  }
}

/**
 * Removes filters that can be evaluated trivially.  This can be done through the following ways:
 * 1) by eliding the filter for cases where it will always evaluate to `true`.
 * 2) by substituting a dummy empty relation when the filter will always evaluate to `false`.
 * 3) by eliminating the always-true conditions given the constraints on the child's output.
 */
object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    case Filter(Literal(false, BooleanType), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    // If any deterministic condition is guaranteed to be true given the constraints on the child's
    // output, remove the condition
    case f @ Filter(fc, p: LogicalPlan) =>
      val (prunedPredicates, remainingPredicates) =
        splitConjunctivePredicates(fc).partition { cond =>
          cond.deterministic && p.constraints.contains(cond)
        }
      if (prunedPredicates.isEmpty) {
        f
      } else if (remainingPredicates.isEmpty) {
        p
      } else {
        val newCond = remainingPredicates.reduce(And)
        Filter(newCond, p)
      }
  }
}

/**
 * 谓词下推遵循如下规律
 * 1) 操作是必须是确定性的
 * 2) 谓词是确定性的，运算符不会更改任何行。
 * 通过case来做各种合法判断
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushDownPredicate extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // SPARK-13473:当Projection输出是不确定性时候，我们是不可以做谓词下推的
    // 因为不确定性表达式会本质上是有状态的。
    // 其实就是，对于给定的输入行，输出由表达式的初始状态和之前处理的所有输入行决定。
    // 换句话说，就是输入行的顺序对于非确定性表示式非常重要，如果非确定性表达式改变了输入行顺序，
    // 那么这种谓词下推是非法的。
    case Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>
      val aliasMap = getAliasMap(project)
      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
    //这种情况也适用于聚合操作。
    case filter @ Filter(condition, aggregate: Aggregate)
      if aggregate.aggregateExpressions.forall(_.deterministic)
        && aggregate.groupingExpressions.nonEmpty =>
      val aliasMap = getAliasMap(aggregate)

      // 对于每个筛选器，展开别名并检查是否可以使用聚合运算符的子运算符生成的属性来计算筛选器。
      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- Aggregate <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }

    // Push [[Filter]] operators through [[Window]] operators. Parts of the predicate that can be
    // 而窗口的谓词下推必须遵循如下条件
    // 1. 所有表达式都是窗口分区键的一部分。表达式可以是复合的。
    // 2. 确定的
    // 3. 置于任何不确定谓词之前。
    case filter @ Filter(condition, w: Window)
      if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))

      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWindow else Filter(stayUp.reduce(And), newWindow)
      } else {
        filter
      }

    case filter @ Filter(condition, union: Union) =>
      // Union可以更改行，因此不能向下推非确定性谓词
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)

      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduceLeft(And)
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet))
          Filter(newCond, grandchild)
        }
        val newUnion = union.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduceLeft(And), newUnion)
        } else {
          newUnion
        }
      } else {
        filter
      }

    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { p =>
        p.deterministic && !p.references.contains(watermark.eventTime)
      }

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduceLeft(And)
        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- watermark <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newWatermark else Filter(stayUp.reduceLeft(And), newWatermark)
      } else {
        filter
      }

    case filter @ Filter(_, u: UnaryNode)
        if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      pushDownPredicate(filter, u.child) { predicate =>
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  def getAliasMap(plan: Project): AttributeMap[Expression] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
    AttributeMap(plan.projectList.collect { case a: Alias => (a.toAttribute, a.child) })
  }

  def getAliasMap(plan: Aggregate): AttributeMap[Expression] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
        (a.toAttribute, a.child)
    }
    AttributeMap(aliasMap)
  }

  def canPushThrough(p: UnaryNode): Boolean = p match {
    // Note that some operators (e.g. project, aggregate, union) are being handled separately
    // (earlier in this rule).
    case _: AppendColumns => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: ScriptTransformation => true
    case _: Sort => true
    case _: BatchEvalPython => true
    case _: ArrowEvalPython => true
    case _ => false
  }

  private def pushDownPredicate(
      filter: Filter,
      grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    // Only push down the predicates that is deterministic and all the referenced attributes
    // come from grandchild.
    // TODO: non-deterministic predicates could be pushed through some operators that do not change
    // the rows.
    val (candidates, nonDeterministic) =
      splitConjunctivePredicates(filter.condition).partition(_.deterministic)

    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }

    val stayUp = rest ++ nonDeterministic

    if (pushDown.nonEmpty) {
      val newChild = insertFilter(pushDown.reduceLeft(And))
      if (stayUp.nonEmpty) {
        Filter(stayUp.reduceLeft(And), newChild)
      } else {
        newChild
      }
    } else {
      filter
    }
  }

  /**
   * Check if we can safely push a filter through a projection, by making sure that predicate
   * subqueries in the condition do not contain the same attributes as the plan they are moved
   * into. This can happen when the plan and predicate subquery have the same source.
   */
  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
    val attributes = plan.outputSet
    val matched = condition.find {
      case s: SubqueryExpression => s.plan.outputSet.intersect(attributes).nonEmpty
      case _ => false
    }
    matched.isEmpty
  }
}

/**
 * 下推[[filter]]运算符，其中“condition”只能使用联接左侧或右侧的属性进行计算。
 * 其他的[[filter]]条件被移动到[[Join]]的“condition”中。
 * 并向下推联接筛选器，在该筛选器中，可以仅使用子查询左侧或右侧的属性（如果适用）来计算“condition”。
 * 
 *
 * 更多细节 ： https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior 
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * 将联接条件表达式或筛选谓词（在给定联接的输出上）拆分为三个基于评估所需属性的类别。
   * 注意，我们明确排除canEvaluateInLeft或CanEvaluateInRight以防止在联接的任一侧推送这些谓词。
   *
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    //T1: a,b,c T2: d,e  
    //select * from T1 join T2 on T1.a = T2.b where T1.b > 3 and T2.e > 4
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    //和左孩子有关系的谓词：T1.b > 3
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))
      //和右孩子有关系的谓词：T2.e > 4

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
     //commonCondition是等于吗？这个commonCondition中是不是会经常没有值，
     //当where中有"="条件的时候，commonCondition会有值
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    // select * from T1 join T2 on T1.a = T2.b where T1.b > 3 and T2.e > 4
    // 以上这个查询，filter是T1.b > 3 and T2.e > 4
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
        //拆分filter的过滤谓词，splitConjunctivePredicates(filterCondition)返回一个expression的数组
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val (newJoinConditions, others) =
            commonFilterCondition.partition(canEvaluateWithinJoin)
          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)
          // 如果是inner join，很简单，直接下推即可
          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left //作为nullable端的左孩子，where谓词是不能下推的
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)//作为nonullable端的右孩子，加入filter然后下推。
          val newJoinCond = joinCondition//joinCondition留在下面再优化
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        //如果存在左孩子谓词和where中的"="的，放在生成join的上面，如果不存在，直接返回join
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    //以上是在处理join上端的filter的下推情况，现在来考虑join中的也就是on之后的filter下推情况
    case j @ Join(left, right, joinType, joinCondition, hint) =>
    //同样对join中on后面的连接谓词分类，分为三类
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _: InnerLike | LeftSemi =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.//如果是inner join，可以下推
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.//如果是inner join，可以下推
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right//右端为非空端，不能下推，只能放在join中作为条件
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
          //最后将右端没有下推的filter和"="的filter一起存入newJoinCond，作为join的条件
          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case FullOuter => j
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case Limit(le, Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}

/**
 * Check if there any cartesian products between joins of any type in the optimized plan tree.
 * Throw an error if a cartesian product is found without an explicit cross join specified.
 * This rule is effectively disabled if the CROSS_JOINS_ENABLED flag is true.
 *
 * This rule must be run AFTER the ReorderJoin rule since the join conditions for each join must be
 * collected before checking if it is a cartesian product. If you have
 * SELECT * from R, S where R.r = S.s,
 * the join between R and S is not a cartesian product and therefore should be allowed.
 * The predicate R.r = S.s is not recognized as a join condition until the ReorderJoin rule.
 *
 * This rule must be run AFTER the batch "LocalRelation", since a join with empty relation should
 * not be a cartesian product.
 */
object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Check if a join is a cartesian product. Returns true if
   * there are no join conditions involving references from both left and right.
   */
  def isCartesianProduct(join: Join): Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)

    conditions match {
      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) => false
      case _ => !conditions.map(_.references).exists(refs =>
        refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.crossJoinEnabled) {
      plan
    } else plan transform {
      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _)
        if isCartesianProduct(j) =>
          throw new AnalysisException(
            s"""Detected implicit cartesian product for ${j.joinType.sql} join between logical plans
               |${left.treeString(false).trim}
               |and
               |${right.treeString(false).trim}
               |Join condition is missing or trivial.
               |Either: use the CROSS JOIN syntax to allow cartesian products between these
               |relations, or: enable implicit cartesian products by setting the configuration
               |variable spark.sql.crossJoin.enabled=true"""
            .stripMargin)
    }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _), _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))),
            prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr =
            we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => we
      }
      case ae @ AggregateExpression(af, _, _, _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => ae
      }
    }
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on `LocalRelation` to
 * another `LocalRelation`.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data, isStreaming))
        if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedMutableProjection(projectList, output)
      projection.initialize(0)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)

    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) =>
      LocalRelation(output, data.take(limit), isStreaming)

    case Filter(condition, LocalRelation(output, data, isStreaming))
        if !hasUnevaluableExpr(condition) =>
      val predicate = InterpretedPredicate.create(condition, output)
      predicate.initialize(0)
      LocalRelation(output, data.filter(row => predicate.eval(row)), isStreaming)
  }

  private def hasUnevaluableExpr(expr: Expression): Boolean = {
    expr.find(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference]).isDefined
  }
}

/**
 * Replaces logical [[Distinct]] operator with an [[Aggregate]] operator.
 * {{{
 *   SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 * }}}
 */
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
 * Replaces logical [[Deduplicate]] operator with an [[Aggregate]] operator.
 */
object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Deduplicate(keys, child) if !child.isStreaming =>
      val keyExprIds = keys.map(_.exprId)
      val aggCols = child.output.map { attr =>
        if (keyExprIds.contains(attr.exprId)) {
          attr
        } else {
          Alias(new First(attr).toAggregateExpression(), attr.name)(attr.exprId)
        }
      }
      // SPARK-22951: Physical aggregate operators distinguishes global aggregation and grouping
      // aggregations by checking the number of grouping keys. The key difference here is that a
      // global aggregation always returns at least one row even if there are no input rows. Here
      // we append a literal when the grouping key list is empty so that the result aggregate
      // operator is properly treated as a grouping aggregation.
      val nonemptyKeys = if (keys.isEmpty) Literal(1) :: Nil else keys
      Aggregate(nonemptyKeys, aggCols, child)
  }
}

/**
 * Replaces logical [[Intersect]] operator with a left-semi [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 INTERSECT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to INTERSECT DISTINCT. Do not use it for INTERSECT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right, false) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE))
  }
}

/**
 * Replaces logical [[Except]] operator with a left-anti [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 EXCEPT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT ANTI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to EXCEPT DISTINCT. Do not use it for EXCEPT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right, false) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE))
  }
}

/**
 * Replaces logical [[Except]] operator using a combination of Union, Aggregate
 * and Generate operator.
 *
 * Input Query :
 * {{{
 *    SELECT c1 FROM ut1 EXCEPT ALL SELECT c1 FROM ut2
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT c1
 *   FROM (
 *     SELECT replicate_rows(sum_val, c1)
 *       FROM (
 *         SELECT c1, sum_val
 *           FROM (
 *             SELECT c1, sum(vcol) AS sum_val
 *               FROM (
 *                 SELECT 1L as vcol, c1 FROM ut1
 *                 UNION ALL
 *                 SELECT -1L as vcol, c1 FROM ut2
 *              ) AS union_all
 *            GROUP BY union_all.c1
 *          )
 *        WHERE sum_val > 0
 *       )
 *   )
 * }}}
 */

object RewriteExceptAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right, true) =>
      assert(left.output.size == right.output.size)

      val newColumnLeft = Alias(Literal(1L), "vcol")()
      val newColumnRight = Alias(Literal(-1L), "vcol")()
      val modifiedLeftPlan = Project(Seq(newColumnLeft) ++ left.output, left)
      val modifiedRightPlan = Project(Seq(newColumnRight) ++ right.output, right)
      val unionPlan = Union(modifiedLeftPlan, modifiedRightPlan)
      val aggSumCol =
        Alias(AggregateExpression(Sum(unionPlan.output.head.toAttribute), Complete, false), "sum")()
      val aggOutputColumns = left.output ++ Seq(aggSumCol)
      val aggregatePlan = Aggregate(left.output, aggOutputColumns, unionPlan)
      val filteredAggPlan = Filter(GreaterThan(aggSumCol.toAttribute, Literal(0L)), aggregatePlan)
      val genRowPlan = Generate(
        ReplicateRows(Seq(aggSumCol.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        filteredAggPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
 * Replaces logical [[Intersect]] operator using a combination of Union, Aggregate
 * and Generate operator.
 *
 * Input Query :
 * {{{
 *    SELECT c1 FROM ut1 INTERSECT ALL SELECT c1 FROM ut2
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT c1
 *   FROM (
 *        SELECT replicate_row(min_count, c1)
 *        FROM (
 *             SELECT c1, If (vcol1_cnt > vcol2_cnt, vcol2_cnt, vcol1_cnt) AS min_count
 *             FROM (
 *                  SELECT   c1, count(vcol1) as vcol1_cnt, count(vcol2) as vcol2_cnt
 *                  FROM (
 *                       SELECT true as vcol1, null as , c1 FROM ut1
 *                       UNION ALL
 *                       SELECT null as vcol1, true as vcol2, c1 FROM ut2
 *                       ) AS union_all
 *                  GROUP BY c1
 *                  HAVING vcol1_cnt >= 1 AND vcol2_cnt >= 1
 *                  )
 *             )
 *         )
 * }}}
 */
object RewriteIntersectAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right, true) =>
      assert(left.output.size == right.output.size)

      val trueVcol1 = Alias(Literal(true), "vcol1")()
      val nullVcol1 = Alias(Literal(null, BooleanType), "vcol1")()

      val trueVcol2 = Alias(Literal(true), "vcol2")()
      val nullVcol2 = Alias(Literal(null, BooleanType), "vcol2")()

      // Add a projection on the top of left and right plans to project out
      // the additional virtual columns.
      val leftPlanWithAddedVirtualCols = Project(Seq(trueVcol1, nullVcol2) ++ left.output, left)
      val rightPlanWithAddedVirtualCols = Project(Seq(nullVcol1, trueVcol2) ++ right.output, right)

      val unionPlan = Union(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols)

      // Expressions to compute count and minimum of both the counts.
      val vCol1AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(0)), Complete, false), "vcol1_count")()
      val vCol2AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(1)), Complete, false), "vcol2_count")()
      val ifExpression = Alias(If(
        GreaterThan(vCol1AggrExpr.toAttribute, vCol2AggrExpr.toAttribute),
        vCol2AggrExpr.toAttribute,
        vCol1AggrExpr.toAttribute
      ), "min_count")()

      val aggregatePlan = Aggregate(left.output,
        Seq(vCol1AggrExpr, vCol2AggrExpr) ++ left.output, unionPlan)
      val filterPlan = Filter(And(GreaterThanOrEqual(vCol1AggrExpr.toAttribute, Literal(1L)),
        GreaterThanOrEqual(vCol2AggrExpr.toAttribute, Literal(1L))), aggregatePlan)
      val projectMinPlan = Project(left.output ++ Seq(ifExpression), filterPlan)

      // Apply the replicator to replicate rows based on min_count
      val genRowPlan = Generate(
        ReplicateRows(Seq(ifExpression.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        projectMinPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
      val newGrouping = grouping.filter(!_.foldable)
      if (newGrouping.nonEmpty) {
        a.copy(groupingExpressions = newGrouping)
      } else {
        // All grouping expressions are literals. We should not drop them all, because this can
        // change the return semantics when the input of the Aggregate is empty (SPARK-17114). We
        // instead replace this by single, easy to hash/sort, literal expression.
        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
      }
  }
}

/**
 * Removes repetition from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.size > 1 =>
      val newGrouping = ExpressionSet(grouping).toSeq
      if (newGrouping.size == grouping.size) {
        a
      } else {
        a.copy(groupingExpressions = newGrouping)
      }
  }
}

/**
 * Replaces GlobalLimit 0 and LocalLimit 0 nodes (subtree) with empty Local Relation, as they don't
 * return any rows.
 */
object OptimizeLimitZero extends Rule[LogicalPlan] {
  // returns empty Local Relation corresponding to given plan
  private def empty(plan: LogicalPlan) =
    LocalRelation(plan.output, data = Seq.empty, isStreaming = plan.isStreaming)

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Nodes below GlobalLimit or LocalLimit can be pruned if the limit value is zero (0).
    // Any subtree in the logical plan that has GlobalLimit 0 or LocalLimit 0 as its root is
    // semantically equivalent to an empty relation.
    //
    // In such cases, the effects of Limit 0 can be propagated through the Logical Plan by replacing
    // the (Global/Local) Limit subtree with an empty LocalRelation, thereby pruning the subtree
    // below and triggering other optimization rules of PropagateEmptyRelation to propagate the
    // changes up the Logical Plan.
    //
    // Replace Global Limit 0 nodes with empty Local Relation
    case gl @ GlobalLimit(IntegerLiteral(0), _) =>
      empty(gl)

    // Note: For all SQL queries, if a LocalLimit 0 node exists in the Logical Plan, then a
    // GlobalLimit 0 node would also exist. Thus, the above case would be sufficient to handle
    // almost all cases. However, if a user explicitly creates a Logical Plan with LocalLimit 0 node
    // then the following rule will handle that case as well.
    //
    // Replace Local Limit 0 nodes with empty Local Relation
    case ll @ LocalLimit(IntegerLiteral(0), _) =>
      empty(ll)
  }
}
