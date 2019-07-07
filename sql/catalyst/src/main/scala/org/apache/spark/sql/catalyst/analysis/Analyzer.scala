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

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, LookupCatalog}
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with a dummy [[SessionCatalog]] and [[EmptyFunctionRegistry]].
 * Used for testing when all relations are already filled in and the analyzer needs only
 * to resolve attribute references.
 */
object SimpleAnalyzer extends Analyzer(
  new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)) {
    override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean) {}
  },
  new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))

/**
 * Provides a way to keep state during the analysis, this enables us to decouple the concerns
 * of analysis environment from the catalog.
 * The state that is kept here is per-query.
 *
 * Note this is thread local.
 *
 * @param defaultDatabase The default database used in the view resolution, this overrules the
 *                        current catalog database.
 * @param nestedViewDepth The nested depth in the view resolution, this enables us to limit the
 *                        depth of nested views.
 */
case class AnalysisContext(
    defaultDatabase: Option[String] = None,
    nestedViewDepth: Int = 0)

object AnalysisContext {
  private val value = new ThreadLocal[AnalysisContext]() {
    override def initialValue: AnalysisContext = AnalysisContext()
  }

  def get: AnalysisContext = value.get()
  def reset(): Unit = value.remove()

  private def set(context: AnalysisContext): Unit = value.set(context)

  def withAnalysisContext[A](database: Option[String])(f: => A): A = {
    val originContext = value.get()
    val context = AnalysisContext(defaultDatabase = database,
      nestedViewDepth = originContext.nestedViewDepth + 1)
    set(context)
    try f finally { set(originContext) }
  }
}

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer(
    catalog: SessionCatalog,
    conf: SQLConf,
    maxIterations: Int)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis with LookupCatalog {

  def this(catalog: SessionCatalog, conf: SQLConf) = {
    this(catalog, conf, conf.optimizerMaxIterations)
  }

  override protected def lookupCatalog(name: String): CatalogPlugin =
    throw new CatalogNotFoundException("No catalog lookup function")

  def executeAndCheck(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    AnalysisHelper.markInAnalyzer {
      val analyzed = executeAndTrack(plan, tracker)
      try {
        checkAnalysis(analyzed)
        analyzed
      } catch {
        case e: AnalysisException =>
          val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
          ae.setStackTrace(e.getStackTrace)
          throw ae
      }
    }
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    AnalysisContext.reset()
    try {
      executeSameContext(plan)
    } finally {
      AnalysisContext.reset()
    }
  }

  private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)

  def resolver: Resolver = conf.resolver

  protected val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Override to provide rules to do post-hoc resolution. Note that these rules will be executed
   * in an individual batch. This batch is to run right after the normal resolution batch and
   * execute its rules in one pass.
   */
  val postHocResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Hints", fixedPoint,
      new ResolveHints.ResolveJoinStrategyHints(conf),
      ResolveHints.ResolveCoalesceHints,
      new ResolveHints.RemoveAllHints(conf)),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(conf)),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
      ResolveTables ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveCreateNamedStruct ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveAggAliasInGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveSubqueryColumnAliases ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ResolveOutputRelation ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables(conf) ::
      ResolveHigherOrderFunctions(catalog) ::
      ResolveLambdaVariables(conf) ::
      ResolveTimeZone(conf) ::
      ResolveRandomSeed ::
      TypeCoercion.typeCoercionRules(conf) ++
      extendedResolutionRules : _*),
    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
    Batch("View", Once,
      AliasViewChild(conf)),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("UpdateNullability", Once,
      UpdateAttributeNullability),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Analyze cte definitions and substitute child plan with analyzed cte definitions.
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case With(child, relations) =>
        // substitute CTE expressions right-to-left to resolve references to previous CTEs:
        // with a as (select * from t), b as (select * from a) select * from b
        relations.foldRight(child) {
          case ((cteName, ctePlan), currentPlan) =>
            substituteCTE(currentPlan, cteName, ctePlan)
        }
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteName: String, ctePlan: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsUp {
        case UnresolvedRelation(Seq(table)) if resolver(cteName, table) =>
          ctePlan
        case u: UnresolvedRelation =>
          u
        case other =>
          // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
          other transformExpressions {
            case e: SubqueryExpression =>
              e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
          }
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) => child.resolveExpressions {
        case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
          val errorMessage =
            s"Window specification $windowName is not defined in the WINDOW clause."
          val windowSpecDefinition =
            windowDefinitions.getOrElse(windowName, failAnalysis(errorMessage))
          WindowExpression(c, windowSpecDefinition)
      }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.map(_.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
          child match {
            case ne: NamedExpression => ne
            case go @ GeneratorOuter(g: Generator) if g.resolved => MultiAlias(go, Nil)
            case e if !e.resolved => u
            case g: Generator => MultiAlias(g, Nil)
            case c @ Cast(ne: NamedExpression, _, _) => Alias(c, ne.name)()
            case e: ExtractValue => Alias(e, toPrettySQL(e))()
            case e if optGenAliasFunc.isDefined =>
              Alias(child, optGenAliasFunc.get.apply(e))()
            case e => Alias(e, toPrettySQL(e))()
          }
        }
      ).asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingSets if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.copy(aggregations = assignAliases(g.aggregations))

      case Pivot(groupByOpt, pivotColumn, pivotValues, aggregates, child)
        if child.resolved && groupByOpt.isDefined && hasUnresolvedAlias(groupByOpt.get) =>
        Pivot(Some(assignAliases(groupByOpt.get)), pivotColumn, pivotValues, aggregates, child)

      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /*
     *  GROUP BY a, b, c WITH ROLLUP
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( ) ).
     *  Group Count: N + 1 (N is the number of group expressions)
     *
     *  We need to get all of its subsets for the rule described above, the subset is
     *  represented as sequence of expressions.
     */
    def rollupExprs(exprs: Seq[Expression]): Seq[Seq[Expression]] = exprs.inits.toIndexedSeq

    /*
     *  GROUP BY a, b, c WITH CUBE
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ) ).
     *  Group Count: 2 ^ N (N is the number of group expressions)
     *
     *  We need to get all of its subsets for a given GROUPBY expression, the subsets are
     *  represented as sequence of expressions.
     */
    def cubeExprs(exprs: Seq[Expression]): Seq[Seq[Expression]] = {
      // `cubeexprs0`是递归的，返回一个懒加载流。
      // 这里我们调用“toindexedseq”来实现它，并避免以后出现序列化问题。
      cubeExprs0(exprs).toIndexedSeq
    }

    def cubeExprs0(exprs: Seq[Expression]): Seq[Seq[Expression]] = exprs.toList match {
      case x :: xs =>
        val initial = cubeExprs0(xs)
        initial.map(x +: _) ++ initial
      case Nil =>
        Seq(Seq.empty)
    }

    private[analysis] def hasGroupingFunction(e: Expression): Boolean = {
      e.collectFirst {
        case g: Grouping => g
        case g: GroupingID => g
      }.isDefined
    }

    private def replaceGroupingFunc(
        expr: Expression,
        groupByExprs: Seq[Expression],
        gid: Expression): Expression = {
      expr transform {
        case e: GroupingID =>
          if (e.groupByExprs.isEmpty || e.groupByExprs == groupByExprs) {
            Alias(gid, toPrettySQL(e))()
          } else {
            throw new AnalysisException(
              s"Columns of grouping_id (${e.groupByExprs.mkString(",")}) does not match " +
                s"grouping columns (${groupByExprs.mkString(",")})")
          }
        case e @ Grouping(col: Expression) =>
          val idx = groupByExprs.indexWhere(_.semanticEquals(col))
          if (idx >= 0) {
            Alias(Cast(BitwiseAnd(ShiftRight(gid, Literal(groupByExprs.length - 1 - idx)),
              Literal(1)), ByteType), toPrettySQL(e))()
          } else {
            throw new AnalysisException(s"Column of grouping ($col) can't be found " +
              s"in grouping columns ${groupByExprs.mkString(",")}")
          }
      }
    }

    /*
     * 为“expand”运算符的所有分组依据表达式创建新别名。
     */
    private def constructGroupByAlias(groupByExprs: Seq[Expression]): Seq[Alias] = {
      groupByExprs.map {
        case e: NamedExpression => Alias(e, e.name)()
        case other => Alias(other, other.toString)()
      }
    }

    /*
     * 使 [[Expand]] 操作 分组集合
     */
    private def constructExpand(
        selectedGroupByExprs: Seq[Seq[Expression]],
        child: LogicalPlan,
        groupByAliases: Seq[Alias],
        gid: Attribute): LogicalPlan = {
      //根据需要更改按别名分组的可空性。例如，如果我们
      //分组集（（a，b），a），我们不需要更改a的可空性，但是我们
      //应将b的nullability更改为true
      //TODO:对于多维数据集/汇总，只需将nullability设置为“true”。
      val expandedAttributes = groupByAliases.map { alias =>
        if (selectedGroupByExprs.exists(!_.contains(alias.child))) {
          alias.toAttribute.withNullability(true)
        } else {
          alias.toAttribute
        }
      }

      val groupingSetsAttributes = selectedGroupByExprs.map { groupingSetExprs =>
        groupingSetExprs.map { expr =>
          val alias = groupByAliases.find(_.child.semanticEquals(expr)).getOrElse(
            failAnalysis(s"$expr doesn't show up in the GROUP BY list $groupByAliases"))
          // Map alias to expanded attribute.
          expandedAttributes.find(_.semanticEquals(alias.toAttribute)).getOrElse(
            alias.toAttribute)
        }
      }

      Expand(groupingSetsAttributes, groupByAliases, expandedAttributes, gid, child)
    }

    /*
     *通过替换分组函数构造新的聚合表达式。
     */
    private def constructAggregateExprs(
        groupByExprs: Seq[Expression],
        aggregations: Seq[NamedExpression],
        groupByAliases: Seq[Alias],
        groupingAttrs: Seq[Expression],
        gid: Attribute): Seq[NamedExpression] = aggregations.map {
      //收集所有找到的AggregateExpression，这样我们可以检查表达式是否是
      //是否有AggregateExpression。
      val aggsBuffer = ArrayBuffer[Expression]()
      //返回表达式是否属于“aggsbuffer”中的任何表达式。
      def isPartOfAggregation(e: Expression): Boolean = {
        aggsBuffer.exists(a => a.find(_ eq e).isDefined)
      }
      replaceGroupingFunc(_, groupByExprs, gid).transformDown {
        //AggregateExpression应根据其参数的未修改值进行计算
        //表达式，因此不应替换对分组表达式的任何引用在里面。
        case e: AggregateExpression =>
          aggsBuffer += e
          e
        case e if isPartOfAggregation(e) => e
        case e =>
          //用扩展输出属性替换表达式。
          val index = groupByAliases.indexWhere(_.child.semanticEquals(e))
          if (index == -1) {
            e
          } else {
            groupingAttrs(index)
          }
      }.asInstanceOf[NamedExpression]
    }

    /*
     * 从多维数据集/汇总/分组集中构造[[Aggregate]]运算符。
     */
    private def constructAggregate(
        selectedGroupByExprs: Seq[Seq[Expression]],
        groupByExprs: Seq[Expression],
        aggregationExprs: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      val gid = AttributeReference(VirtualColumn.groupingIdName, IntegerType, false)()

      // 对于符合ANSI-SQL的分组集语法，groupbyexprs是可选的，可以为空。在这种情况下，
      // 我们从用户为分组集提供的值派生groupByExpr。
      val finalGroupByExpressions = if (groupByExprs == Nil) {
        selectedGroupByExprs.flatten.foldLeft(Seq.empty[Expression]) { (result, currentExpr) =>
          //Group By表达式中只包含唯一表达式，并确定
          //基于它们的语义相等性。例子。分组集（（a*b），（b*a））结果
          //在分组表达式（A*B）中
          if (result.find(_.semanticEquals(currentExpr)).isDefined) {
            result
          } else {
            result :+ currentExpr
          }
        }
      } else {
        groupByExprs
      }

      //通过将分组表达式设置为空（由
      //`选择edGroupByExprs`。防止在聚合中使用这些空值
      //我们需要为所有group by表达式创建新的别名，而不是原始值
      //仅用于预期目的。
      val groupByAliases = constructGroupByAlias(finalGroupByExpressions)

      val expand = constructExpand(selectedGroupByExprs, child, groupByAliases, gid)
      val groupingAttrs = expand.output.drop(child.output.length)

      val aggregations = constructAggregateExprs(
        finalGroupByExpressions, aggregationExprs, groupByAliases, groupingAttrs, gid)

      Aggregate(groupingAttrs, aggregations, expand)
    }

    private def findGroupingExprs(plan: LogicalPlan): Seq[Expression] = {
      plan.collectFirst {
        case a: Aggregate =>
          // this Aggregate should have grouping id as the last grouping key.
          val gid = a.groupingExpressions.last
          if (!gid.isInstanceOf[AttributeReference]
            || gid.asInstanceOf[AttributeReference].name != VirtualColumn.groupingIdName) {
            failAnalysis(s"grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
          }
          a.groupingExpressions.take(a.groupingExpressions.length - 1)
      }.getOrElse {
        failAnalysis(s"grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
      }
    }

    // This require transformUp to replace grouping()/grouping_id() in resolved Filter/Sort
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.

      // Ensure group by expressions and aggregate expressions have been resolved.
      case Aggregate(Seq(c @ Cube(groupByExprs)), aggregateExpressions, child)
        if (groupByExprs ++ aggregateExpressions).forall(_.resolved) =>
        constructAggregate(cubeExprs(groupByExprs), groupByExprs, aggregateExpressions, child)
      case Aggregate(Seq(r @ Rollup(groupByExprs)), aggregateExpressions, child)
        if (groupByExprs ++ aggregateExpressions).forall(_.resolved) =>
        constructAggregate(rollupExprs(groupByExprs), groupByExprs, aggregateExpressions, child)
      // Ensure all the expressions have been resolved.
      case x: GroupingSets if x.expressions.forall(_.resolved) =>
        constructAggregate(x.selectedGroupByExprs, x.groupByExprs, x.aggregations, x.child)

      // We should make sure all expressions in condition have been resolved.
      case f @ Filter(cond, child) if hasGroupingFunction(cond) && cond.resolved =>
        val groupingExprs = findGroupingExprs(child)
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newCond = replaceGroupingFunc(cond, groupingExprs, VirtualColumn.groupingIdAttribute)
        f.copy(condition = newCond)

      // We should make sure all [[SortOrder]]s have been resolved.
      case s @ Sort(order, _, child)
        if order.exists(hasGroupingFunction) && order.forall(_.resolved) =>
        val groupingExprs = findGroupingExprs(child)
        val gid = VirtualColumn.groupingIdAttribute
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newOrder = order.map(replaceGroupingFunc(_, groupingExprs, gid).asInstanceOf[SortOrder])
        s.copy(order = newOrder)
    }
  }

  object ResolvePivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: Pivot if !p.childrenResolved || !p.aggregates.forall(_.resolved)
        || (p.groupByExprsOpt.isDefined && !p.groupByExprsOpt.get.forall(_.resolved))
        || !p.pivotColumn.resolved || !p.pivotValues.forall(_.resolved) => p
      case Pivot(groupByExprsOpt, pivotColumn, pivotValues, aggregates, child) =>
        if (!RowOrdering.isOrderable(pivotColumn.dataType)) {
          throw new AnalysisException(
            s"Invalid pivot column '${pivotColumn}'. Pivot columns must be comparable.")
        }
        // 检查所有聚合表达式
        aggregates.foreach(checkValidAggregateExpression)
        // 检查所有透视值是否为文本并匹配透视列数据类型。
        val evalPivotValues = pivotValues.map { value =>
          val foldable = value match {
            case Alias(v, _) => v.foldable
            case _ => value.foldable
          }
          if (!foldable) {
            throw new AnalysisException(
              s"Literal expressions required for pivot values, found '$value'")
          }
          if (!Cast.canCast(value.dataType, pivotColumn.dataType)) {
            throw new AnalysisException(s"Invalid pivot value '$value': " +
              s"value data type ${value.dataType.simpleString} does not match " +
              s"pivot column data type ${pivotColumn.dataType.catalogString}")
          }
          Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
        }
        //来自SQL的group-by表达式是隐式的，需要推导。
        val groupByExprs = groupByExprsOpt.getOrElse {
          val pivotColAndAggRefs = pivotColumn.references ++ AttributeSet(aggregates)
          child.output.filterNot(pivotColAndAggRefs.contains)
        }
        val singleAgg = aggregates.size == 1
        def outputName(value: Expression, aggregate: Expression): String = {
          val stringValue = value match {
            case n: NamedExpression => n.name
            case _ =>
              val utf8Value =
                Cast(value, StringType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
              Option(utf8Value).map(_.toString).getOrElse("null")
          }
          if (singleAgg) {
            stringValue
          } else {
            val suffix = aggregate match {
              case n: NamedExpression => n.name
              case _ => toPrettySQL(aggregate)
            }
            stringValue + "_" + suffix
          }
        }
        if (aggregates.forall(a => PivotFirst.supportsDataType(a.dataType))) {
          // 因为对每个输入行的PivotValues if语句进行计算会变慢，所以这是一个
          // 使用两个聚合步骤的备用计划。
          val namedAggExps: Seq[NamedExpression] = aggregates.map(a => Alias(a, a.sql)())
          val namedPivotCol = pivotColumn match {
            case n: NamedExpression => n
            case _ => Alias(pivotColumn, "__pivot_col")()
          }
          val bigGroup = groupByExprs :+ namedPivotCol
          val firstAgg = Aggregate(bigGroup, bigGroup ++ namedAggExps, child)
          val pivotAggs = namedAggExps.map { a =>
            Alias(PivotFirst(namedPivotCol.toAttribute, a.toAttribute, evalPivotValues)
              .toAggregateExpression()
            , "__pivot_" + a.sql)()
          }
          val groupByExprsAttr = groupByExprs.map(_.toAttribute)
          val secondAgg = Aggregate(groupByExprsAttr, groupByExprsAttr ++ pivotAggs, firstAgg)
          val pivotAggAttribute = pivotAggs.map(_.toAttribute)
          val pivotOutputs = pivotValues.zipWithIndex.flatMap { case (value, i) =>
            aggregates.zip(pivotAggAttribute).map { case (aggregate, pivotAtt) =>
              Alias(ExtractValue(pivotAtt, Literal(i), resolver), outputName(value, aggregate))()
            }
          }
          Project(groupByExprsAttr ++ pivotOutputs, secondAgg)
        } else {
          val pivotAggregates: Seq[NamedExpression] = pivotValues.flatMap { value =>
            def ifExpr(e: Expression) = {
              If(
                EqualNullSafe(
                  pivotColumn,
                  Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone))),
                e, Literal(null))
            }
            aggregates.map { aggregate =>
              val filteredAggregate = aggregate.transformDown {
                // 假设是聚合函数忽略空值。对于所有当前的aggregateFunction都是这样的，
                // 除了第一个和最后一个处于默认模式（我们处理）的函数，可能还有一些配置单元udaf。
                case First(expr, _) =>
                  First(ifExpr(expr), Literal(true))
                case Last(expr, _) =>
                  Last(ifExpr(expr), Literal(true))
                case a: AggregateFunction =>
                  a.withNewChildren(a.children.map(ifExpr))
              }.transform {
                //我们正在复制聚合，这些聚合现在正在为每个聚合计算不同的值透视值。
                //TODO:在分析之后才构造物理容器。
                case ae: AggregateExpression => ae.copy(resultId = NamedExpression.newExprId)
              }
              Alias(filteredAggregate, outputName(value, aggregate))()
            }
          }
          Aggregate(groupByExprs, groupByExprs ++ pivotAggregates, child)
        }
    }

    //支持聚合计划中可以出现的任何聚合表达式，熊猫UDF除外。
    //TODO:支持pandasUDF。
    private def checkValidAggregateExpression(expr: Expression): Unit = expr match {
      case _: AggregateExpression => // OK and leave the argument check to CheckAnalysis.
      case expr: PythonUDF if PythonUDF.isGroupedAggPandasUDF(expr) =>
        failAnalysis("Pandas UDF aggregate expressions are currently not supported in pivot.")
      case e: Attribute =>
        failAnalysis(
          s"Aggregate expression required for pivot, but '${e.sql}' " +
          s"did not appear in any aggregate function.")
      case e => e.children.foreach(checkValidAggregateExpression)
    }
  }

  /**
   * Resolve table relations with concrete relations from v2 catalog.
   *
   * [[ResolveRelations]] still resolves v1 tables.
   */
  object ResolveTables extends Rule[LogicalPlan] {
    import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util._

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u @ UnresolvedRelation(CatalogObjectIdentifier(Some(catalogPlugin), ident)) =>
        loadTable(catalogPlugin, ident).map(DataSourceV2Relation.create).getOrElse(u)
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {

    // If the unresolved relation is running directly on files, we just return the original
    // UnresolvedRelation, the plan will get resolved later. Else we look up the table from catalog
    // and change the default database name(in AnalysisContext) if it is a view.
    // We usually look up a table from the default database if the table identifier has an empty
    // database part, for a view the default database should be the currentDb when the view was
    // created. When the case comes to resolving a nested view, the view may have different default
    // database with that the referenced view has, so we need to use
    // `AnalysisContext.defaultDatabase` to track the current default database.
    // When the relation we resolve is a view, we fetch the view.desc(which is a CatalogTable), and
    // then set the value of `CatalogTable.viewDefaultDatabase` to
    // `AnalysisContext.defaultDatabase`, we look up the relations that the view references using
    // the default database.
    // For example:
    // |- view1 (defaultDatabase = db1)
    //   |- operator
    //     |- table2 (defaultDatabase = db1)
    //     |- view2 (defaultDatabase = db2)
    //        |- view3 (defaultDatabase = db3)
    //   |- view4 (defaultDatabase = db4)
    // In this case, the view `view1` is a nested view, it directly references `table2`, `view2`
    // and `view4`, the view `view2` references `view3`. On resolving the table, we look up the
    // relations `table2`, `view2`, `view4` using the default database `db1`, and look up the
    // relation `view3` using the default database `db2`.
    //
    // Note this is compatible with the views defined by older versions of Spark(before 2.2), which
    // have empty defaultDatabase and all the relations in viewText have database part defined.
    def resolveRelation(plan: LogicalPlan): LogicalPlan = plan match {
      //若为UnresolvedRelation，则调用resolveRelation方法进行解析：
      //不是这种情况 select * from parquet.`/path/to/query`
      case u @ UnresolvedRelation(AsTableIdentifier(ident)) if !isRunningDirectlyOnFiles(ident) =>
        val defaultDatabase = AnalysisContext.get.defaultDatabase//获取数据名称
        val foundRelation = lookupTableFromCatalog(ident, u, defaultDatabase)
        if (foundRelation != u) {
          resolveRelation(foundRelation)
        } else {
          u
        }

      // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
      // `viewText` should be defined, or else we throw an error on the generation of the View
      // operator.
      case view @ View(desc, _, child) if !child.resolved =>
        // Resolve all the UnresolvedRelations and Views in the child.
        val newChild = AnalysisContext.withAnalysisContext(desc.viewDefaultDatabase) {
          if (AnalysisContext.get.nestedViewDepth > conf.maxNestedViewDepth) {
            view.failAnalysis(s"The depth of view ${view.desc.identifier} exceeds the maximum " +
              s"view resolution depth (${conf.maxNestedViewDepth}). Analysis is aborted to " +
              s"avoid errors. Increase the value of ${SQLConf.MAX_NESTED_VIEW_DEPTH.key} to work " +
              "around this.")
          }
          executeSameContext(child)
        }
        view.copy(child = newChild)
      case p @ SubqueryAlias(_, view: View) =>
        val newChild = resolveRelation(view)
        p.copy(child = newChild)
      case _ => plan
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case i @ InsertIntoTable(u @ UnresolvedRelation(AsTableIdentifier(ident)), _, child, _, _)
          if child.resolved =>
        EliminateSubqueryAliases(lookupTableFromCatalog(ident, u)) match {
          case v: View =>
            u.failAnalysis(s"Inserting into a view is not allowed. View: ${v.desc.identifier}.")
          case other => i.copy(table = other)
        }
      case u: UnresolvedRelation => resolveRelation(u)
    }

    // Look up the table with the given name from catalog. The database we used is decided by the
    // precedence:
    // 1. Use the database part of the table identifier, if it is defined;
    // 2. Use defaultDatabase, if it is defined(In this case, no temporary objects can be used,
    //    and the default database is only used to look up a view);
    // 3. Use the currentDb of the SessionCatalog.
    private def lookupTableFromCatalog(
        tableIdentifier: TableIdentifier,
        u: UnresolvedRelation,
        defaultDatabase: Option[String] = None): LogicalPlan = {
      val tableIdentWithDb = tableIdentifier.copy(
        database = tableIdentifier.database.orElse(defaultDatabase))
      try {
        catalog.lookupRelation(tableIdentWithDb)
      } catch {
        case _: NoSuchTableException | _: NoSuchDatabaseException =>
          u
      }
    }

    // If the database part is specified, and we support running SQL directly on files, and
    // it's not a temporary view, and the table does not exist, then let's just return the
    // original UnresolvedRelation. It is possible we are matching a query like "select *
    // from parquet.`/path/to/query`". The plan will get resolved in the rule `ResolveDataSource`.
    // Note that we are testing (!db_exists || !table_exists) because the catalog throws
    // an exception from tableExists if the database does not exist.
    private def isRunningDirectlyOnFiles(table: TableIdentifier): Boolean = {
      table.database.isDefined && conf.runSQLonFile && !catalog.isTemporaryTable(table) &&
        (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))
    }
  }

  /**
   * 用来自的具体的[[attributereference]]s替换[[unsolvedattribute]]s逻辑计划节点的子节点。
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    /**
     * 为具有不同表达式ID的右子级生成新的逻辑计划对于所有冲突的属性。
     * 入参是左逻辑计划，右逻辑计划，防返回一个新逻辑计划
     */
    private def dedupRight (left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
      //属性冲突的变量
      val conflictingAttributes = left.outputSet.intersect(right.outputSet)
      logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} " +
        s"between $left and $right")

      right.collect {
        // 处理可能出现多次的基本关系。
        case oldVersion: MultiInstanceRelation
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          val newVersion = oldVersion.newInstance()
          (oldVersion, newVersion)

        case oldVersion: SerializeFromObject
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(serializer = oldVersion.serializer.map(_.newInstance())))

        // 处理创建冲突别名的项目。
        case oldVersion @ Project(projectList, _)
            if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

        case oldVersion @ Aggregate(_, aggregateExpressions, _)
            if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

        case oldVersion @ FlatMapGroupsInPandas(_, _, output, _)
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(output = output.map(_.newInstance())))

        case oldVersion: Generate
            if oldVersion.producedAttributes.intersect(conflictingAttributes).nonEmpty =>
          val newOutput = oldVersion.generatorOutput.map(_.newInstance())
          (oldVersion, oldVersion.copy(generatorOutput = newOutput))

        case oldVersion @ Window(windowExpressions, _, _, child)
            if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
              .nonEmpty =>
          (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))
      }
        // 只处理第一种情况，其他情况将在下一个通行证上解决。
        .headOption match {
        case None =>
          /*
           * 没有结果意味着有一个逻辑计划节点产生新的引用
           * 这条规则无法处理。如果是这样的话，一定还有另外一条规则
           * 解决了这些冲突。否则，分析将失败。
           */
          right
        case Some((oldRelation, newRelation)) =>
          val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
          right transformUp {
            case r if r == oldRelation => newRelation
          } transformUp {
            case other => other transformExpressions {
              case a: Attribute =>
                dedupAttr(a, attributeRewrites)
              case s: SubqueryExpression =>
                s.withNewPlan(dedupOuterReferencesInSubquery(s.plan, attributeRewrites))
            }
          }
      }
    }

    private def dedupAttr(attr: Attribute, attrMap: AttributeMap[Attribute]): Attribute = {
      val exprId = attrMap.getOrElse(attr, attr).exprId
      attr.withExprId(exprId)
    }

    /**
     * 
     * 外部计划可能已被消除重复，下面的功能更新外部引用以引用消除重复的属性。
     *
     * For example (SQL):
     * {{{
     *   SELECT * FROM t1
     *   INTERSECT
     *   SELECT * FROM t1
     *   WHERE EXISTS (SELECT 1
     *                 FROM t2
     *                 WHERE t1.c1 = t2.c1)
     * }}}
     * Plan before resolveReference rule.
     *    'Intersect
     *    :- Project [c1#245, c2#246]
     *    :  +- SubqueryAlias t1
     *    :     +- Relation[c1#245,c2#246] parquet
     *    +- 'Project [*]
     *       +- Filter exists#257 [c1#245]
     *       :  +- Project [1 AS 1#258]
     *       :     +- Filter (outer(c1#245) = c1#251)
     *       :        +- SubqueryAlias t2
     *       :           +- Relation[c1#251,c2#252] parquet
     *       +- SubqueryAlias t1
     *          +- Relation[c1#245,c2#246] parquet
     * Plan after the resolveReference rule.
     *    Intersect
     *    :- Project [c1#245, c2#246]
     *    :  +- SubqueryAlias t1
     *    :     +- Relation[c1#245,c2#246] parquet
     *    +- Project [c1#259, c2#260]
     *       +- Filter exists#257 [c1#259]
     *       :  +- Project [1 AS 1#258]
     *       :     +- Filter (outer(c1#259) = c1#251) => Updated
     *       :        +- SubqueryAlias t2
     *       :           +- Relation[c1#251,c2#252] parquet
     *       +- SubqueryAlias t1
     *          +- Relation[c1#259,c2#260] parquet  => Outer plan's attributes are de-duplicated.
     */
    private def dedupOuterReferencesInSubquery(
        plan: LogicalPlan,
        attrMap: AttributeMap[Attribute]): LogicalPlan = {
      plan transformDown { case currentFragment =>
        currentFragment transformExpressions {
          case OuterReference(a: Attribute) =>
            OuterReference(dedupAttr(a, attrMap))
          case s: SubqueryExpression =>
            s.withNewPlan(dedupOuterReferencesInSubquery(s.plan, attrMap))
        }
      }
    }

    /**
     * 通过自顶向下遍历输入表达式来解析属性并提取值表达式。
     * 他以自上而下的方式 我们需要跳过未绑定的lamda函数表达式。lamda的表达是在另一个规则中解析[[resolvelambdavariables]]
     *
     * Example :
     * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
     *
     * In the case above, x and i are resolved as lamda variables in [[ResolveLambdaVariables]]
     * 在这个例程中，未解析的属性是从输入计划的子属性。
     */
    private def resolveExpressionTopDown(e: Expression, q: LogicalPlan): Expression = {
      if (e.resolved) return e
      e match {
        case f: LambdaFunction if !f.bound => f
        case u @ UnresolvedAttribute(nameParts) =>
          // 如果解决失败，请保持不变。希望下一轮能解决。
          val result =
            withPosition(u) {
              q.resolveChildren(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, q))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, resolver)
        case _ => e.mapChildren(resolveExpressionTopDown(_, q))
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // 逻辑计划
      case p: LogicalPlan if !p.childrenResolved => p

      // 如果projection包含Stars,则展开
      case p: Project if containsStar(p.projectList) =>
        p.copy(projectList = buildExpandedProjectList(p.projectList, p.child))
      // 如果aggregate函数的参数包含Stars,则展开
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        if (a.groupingExpressions.exists(_.isInstanceOf[UnresolvedOrdinal])) {
          failAnalysis(
            "Star (*) is not allowed in select list when GROUP BY ordinal position is used")
        } else {
          a.copy(aggregateExpressions = buildExpandedProjectList(a.aggregateExpressions, a.child))
        }
      // 如果转换脚本入参包含Stars,则展开
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child, resolver)
            case o => o :: Nil
          }
        )
      //如果生成子逻辑中包含Stars,则展开
      case g: Generate if containsStar(g.generator.children) =>
        //非法检查
        failAnalysis("Invalid usage of '*' in explode/json_tuple/UDTF")

      // 解析join和intersect的重复表达式ID
      case j @ Join(left, right, _, _, _) if !j.duplicateResolved =>
        j.copy(right = dedupRight(left, right))
      case i @ Intersect(left, right, _) if !i.duplicateResolved =>
        i.copy(right = dedupRight(left, right))
      case e @ Except(left, right, _) if !e.duplicateResolved =>
        e.copy(right = dedupRight(left, right))
      case u @ Union(children) if !u.duplicateResolved =>
        // Use projection-based de-duplication for Union to avoid breaking the checkpoint sharing
        // feature in streaming.
        val newChildren = children.foldRight(Seq.empty[LogicalPlan]) { (head, tail) =>
          head +: tail.map {
            case child if head.outputSet.intersect(child.outputSet).isEmpty =>
              child
            case child =>
              val projectList = child.output.map { attr =>
                Alias(attr, attr.name)()
              }
              Project(projectList, child)
          }
        }
        u.copy(children = newChildren)

      // 在基于子级的排序中解析“sortorder”时，不要将错误报告
      // 因为我们仍然有机会根据它的后代来解决它
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering =
          ordering.map(order => resolveExpressionBottomUp(order, child).asInstanceOf[SortOrder])
        Sort(newOrdering, global, child)

      //生成的特殊情况，因为生成的输出不应通过解析器引用。输出中的属性将由resolvegenerate解析。
      case g @ Generate(generator, _, _, _, _, _) if generator.resolved => g

      case g @ Generate(generator, join, outer, qualifier, output, child) =>
        val newG = resolveExpressionBottomUp(generator, child, throws = true)
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      // 跳过包含反序列化程序表达式的计划，因为它们应该由另一个规则：resolvederializer。
      case plan if containsDeserializer(plan.expressions) => plan

      // SPARK-25942: 解析具有“appendcolumns”子级的聚合表达式而不是
      // `AppendColumns`，因为'AppendColumns`'的序列化程序可能会产生冲突属性
      // 导致不明确引用异常的名称。
      case a @ Aggregate(groupingExprs, aggExprs, appendColumns: AppendColumns) =>
        a.mapExpressions(resolveExpressionTopDown(_, appendColumns))

      case o: OverwriteByExpression if !o.outputResolved =>
      // 在对查询属性进行解析之前，不要解析表达式属性
      // 由resolveOutputRelation创建的表。该规则将属性别名为表的名称。
        o

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString(SQLConf.get.maxToStringFields)}")
        q.mapExpressions(resolveExpressionTopDown(_, q))
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }

    /**
     * 为 Project/Aggregate 构建一个项目列表，并尽可能扩展
     */
    private def buildExpandedProjectList(
      exprs: Seq[NamedExpression],
      child: LogicalPlan): Seq[NamedExpression] = {
      exprs.flatMap {
        // Using Dataframe/Dataset API: testData2.groupBy($"a", $"b").agg($"*")
        case s: Star => s.expand(child, resolver)
        // Using SQL API without running ResolveAlias: SELECT * FROM testData2 group by a, b
        case UnresolvedAlias(s: Star, _) => s.expand(child, resolver)
        case o if containsStar(o :: Nil) => expandStarExpression(o, child) :: Nil
        case o => o :: Nil
      }.map(_.asInstanceOf[NamedExpression])
    }

    /**
     * 如果“exprs”包含[[Star]]，则返回true。
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)

    /**
     * 在子节点中展开匹配的*'s的属性
     */
    def expandStarExpression(expr: Expression, child: LogicalPlan): Expression = {
      expr.transformUp {
        case f1: UnresolvedFunction if containsStar(f1.children) =>
          f1.copy(children = f1.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case c: CreateNamedStruct if containsStar(c.valExprs) =>
          val newChildren = c.children.grouped(2).flatMap {
            case Seq(k, s : Star) => CreateStruct(s.expand(child, resolver)).children
            case kv => kv
          }
          c.copy(children = newChildren.toList )
        case c: CreateArray if containsStar(c.children) =>
          c.copy(children = c.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case p: Murmur3Hash if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case p: XxHash64 if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        // count(*) has been replaced by count(1)
        case o if containsStar(o.children) =>
          failAnalysis(s"Invalid usage of '*' in expression '${o.prettyName}'")
      }
    }
  }

  private def containsDeserializer(exprs: Seq[Expression]): Boolean = {
    exprs.exists(_.find(_.isInstanceOf[UnresolvedDeserializer]).isDefined)
  }

  /**
   * 当属性不可解析时，文本函数不要求用户在调用它们时指定大括号，我们尝试将其解析为文本函数。
   */
  private def resolveLiteralFunction(
      nameParts: Seq[String],
      attribute: UnresolvedAttribute,
      plan: LogicalPlan): Option[Expression] = {
    //如果入参nameParts!=1说明参数异常
    if (nameParts.length != 1) return None
    //判断是否是命名好的表达式，boolean类型
    val isNamedExpression = plan match {
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions.contains(attribute)
      case Project(projectList, _) => projectList.contains(attribute)
      case Window(windowExpressions, _, _, _) => windowExpressions.contains(attribute)
      case _ => false
    }
    val wrapper: Expression => Expression =
      if (isNamedExpression) f => Alias(f, toPrettySQL(f))() else identity
    // 支持当前日期和当前时间戳
    val literalFunctions = Seq(CurrentDate(), CurrentTimestamp())
    val name = nameParts.head
    val func = literalFunctions.find(e => caseInsensitiveResolution(e.prettyName, name))
    func.map(wrapper)
  }

  /**
   * 通过遍历自下而上的输入表达式。为了解析嵌套的复杂类型字段
   * 或者说，此函数使用“throws”参数控制何时引发分析例外。
   *
   * Example :
   * SELECT a.b FROM t ORDER BY b[0].d
   *
   * 在上面的例子中，需要先解决B中的问题，然后才能解决D中的问题。鉴于我们执行自下而上的遍历时
   * 它将首先尝试解析d并失败，因为b没有解决。
   * 如果“throws”为false，则此函数将通过返回原始属性来处理异常。在这种情况下，“d”将在“b”解析之后的后续过程中解析。
   */
  protected[sql] def resolveExpressionBottomUp(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false): Expression = {
    if (expr.resolved) return expr
    // 在一轮中解析表达式。
    // 如果throws == false 或者 the 想要属性不存在
    // (如 尝试去解析 `a.b` 但是 `a` 不存在), 失败返回原始expr
    // 否则抛出异常
    try {
      expr transformUp {
        case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          val result =
            withPosition(u) {
              plan.resolve(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, plan))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldName) if child.resolved =>
          ExtractValue(child, fieldName, resolver)
      }
    } catch {
      case a: AnalysisException if !throws => expr
    }
  }

  /**
   * 在许多SQL语法中在ORDER/SORT BY和GROUP BY子句中使用顺序位置是有效的。此规则用于将序号位置转换为选择列表中相应的表达式。
   * Spark 2.0中引入了此支持。
   *
   * - 当时sort或者groupby不是整型，但是foldable表达式就忽略
   * - 当spark.sql.orderByOrdinal/spark.sql.groupByOrdinal 参数是false, 也同样忽略
   *
   * Spark 2.0版本之前, order/sort by and group by 是无效的
   */
  object ResolveOrdinalInOrderByAndGroupBy extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      // 将索引替换为order by的相关属性，
      // 它是投影列表的1个基位置。
      case Sort(orders, global, child)
        if orders.exists(_.child.isInstanceOf[UnresolvedOrdinal]) =>
        val newOrders = orders map {
          case s @ SortOrder(UnresolvedOrdinal(index), direction, nullOrdering, _) =>
            if (index > 0 && index <= child.output.size) {
              SortOrder(child.output(index - 1), direction, nullOrdering, Set.empty)
            } else {
              s.failAnalysis(
                s"ORDER BY position $index is not in select list " +
                  s"(valid range is [1, ${child.output.size}])")
            }
          case o => o
        }
        Sort(newOrders, global, child)

      // 将索引替换为AggregateExpressions中的相应表达式。索引是
      //AggregateExpressions的1个基位置，即输出列（Select Expression）
      case Aggregate(groups, aggs, child) if aggs.forall(_.resolved) &&
        groups.exists(_.isInstanceOf[UnresolvedOrdinal]) =>
        val newGroups = groups.map {
          case u @ UnresolvedOrdinal(index) if index > 0 && index <= aggs.size =>
            aggs(index - 1)
          case ordinal @ UnresolvedOrdinal(index) =>
            ordinal.failAnalysis(
              s"GROUP BY position $index is not in select list " +
                s"(valid range is [1, ${aggs.size}])")
          case o => o
        }
        Aggregate(newGroups, aggs, child)
    }
  }

  /**
   * 将分组键中的未解析表达式替换为select子句中的已解析表达式。
   * 此规则应在应用了[[冲突解决程序引用]]之后运行。
   */
  object ResolveAggAliasInGroupBy extends Rule[LogicalPlan] {

    // 不过，这是一个严格的检查，我们将此设置为仅在子表达式不可解析时应用规则。
    private def notResolvableByChild(attrName: String, child: LogicalPlan): Boolean = {
      !child.output.exists(a => resolver(a.name, attrName))
    }

    private def mayResolveAttrByAggregateExprs(
        exprs: Seq[Expression], aggs: Seq[NamedExpression], child: LogicalPlan): Seq[Expression] = {
      exprs.map { _.transform {
        case u: UnresolvedAttribute if notResolvableByChild(u.name, child) =>
          aggs.find(ne => resolver(ne.name, u.name)).getOrElse(u)
      }}
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      //如果是聚合
      case agg @ Aggregate(groups, aggs, child)
          if conf.groupByAliases && child.resolved && aggs.forall(_.resolved) &&
            groups.exists(!_.resolved) =>
        agg.copy(groupingExpressions = mayResolveAttrByAggregateExprs(groups, aggs, child))
      //如果是分组集合
      case gs @ GroupingSets(selectedGroups, groups, child, aggs)
          if conf.groupByAliases && child.resolved && aggs.forall(_.resolved) &&
            groups.exists(_.isInstanceOf[UnresolvedAttribute]) =>
        gs.copy(
          selectedGroupByExprs = selectedGroups.map(mayResolveAttrByAggregateExprs(_, aggs, child)),
          groupByExprs = mayResolveAttrByAggregateExprs(groups, aggs, child))
    }
  }

  /**
   * 在常规的SQL语法规则中，有很多sort的语法是没有在select语句中体现出来的。
   * 这个方法就是发现这种查询，同时把这种查询属性补上到常规的projection
   * 保证能完整有序的排序。 添加另一个projection以在排序后删除这些属性。
   *
   * HAVING子句还可以使用SELECT中未显示的分组列。
   */
  object ResolveMissingReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // 跳过聚合排序。这将在ResolveAgregateFunctions中处理
      case sa @ Sort(_, _, child: Aggregate) => sa

      case s @ Sort(order, _, child)
          if (!s.resolved || s.missingInput.nonEmpty) && child.resolved =>
        val (newOrder, newChild) = resolveExprsAndAddMissingAttrs(order, child)
        val ordering = newOrder.map(_.asInstanceOf[SortOrder])
        if (child.output == newChild.output) {
          s.copy(order = ordering)
        } else {
          //添加缺少的属性，然后将它们投影掉。
          val newSort = s.copy(order = ordering, child = newChild)
          Project(child.output, newSort)
        }

      case f @ Filter(cond, child) if (!f.resolved || f.missingInput.nonEmpty) && child.resolved =>
        val (newCond, newChild) = resolveExprsAndAddMissingAttrs(Seq(cond), child)
        if (child.output == newChild.output) {
          f.copy(condition = newCond.head)
        } else {
          // 添加缺少的属性，然后将它们投影掉。
          val newFilter = Filter(newCond.head, newChild)
          Project(child.output, newFilter)
        }
    }

    /**
     * 此方法尝试递归解析表达式并查找缺少的属性。
     * 特别是当“sort”或“filter”中使用的表达式包含未解析的属性或子输出中缺少的已解析属性时。
     * 此方法试图找出缺少的属性并将其添加到投影中。
     */
    private def resolveExprsAndAddMissingAttrs(
        exprs: Seq[Expression], plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
      
      // 缺少的属性可以是未解析的属性或未在计划的输出属性中解析的属性。
      if (exprs.forall(e => e.resolved && e.references.subsetOf(plan.outputSet))) {
        (exprs, plan)
      } else {
        plan match {
          case p: Project =>
            // 正在根据当前计划解析表达式。
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, p))
            // Recursively resolving expressions on the child of current plan.
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, p.child)
            // 如果表达式使用的某些属性只能在重写的子计划上解析，则需要将它们添加到原始投影中。
            val missingAttrs = (AttributeSet(newExprs) -- p.outputSet).intersect(newChild.outputSet)
            (newExprs, Project(p.projectList ++ missingAttrs, newChild))

          case a @ Aggregate(groupExprs, aggExprs, child) =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, a))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, child)
            val missingAttrs = (AttributeSet(newExprs) -- a.outputSet).intersect(newChild.outputSet)
            if (missingAttrs.forall(attr => groupExprs.exists(_.semanticEquals(attr)))) {
              // 所有缺少的属性都是分组表达式，有效大小写。
              (newExprs, a.copy(aggregateExpressions = aggExprs ++ missingAttrs, child = newChild))
            } else {
              // 需要添加非分组属性，大小写无效。
              (exprs, a)
            }

          case g: Generate =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, g))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, g.child)
            (newExprs, g.copy(unrequiredChildIndex = Nil, child = newChild))

          // 对于“distinct”和“subqueryalias”，我们无法通过其子级递归解析和添加属性。
          case u: UnaryNode if !u.isInstanceOf[Distinct] && !u.isInstanceOf[SubqueryAlias] =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, u))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, u.child)
            (newExprs, u.withNewChildren(Seq(newChild)))

          // 对于其他运算符，我们不能通过其子级递归解析和添加属性。
          case other =>
            (exprs.map(resolveExpressionBottomUp(_, other)), other)
        }
      }
    }
  }

  /**
   * 检查函数注册表中是否定义了由[[UnsolvedFunction]引用的函数标识符
   * 请注意，此规则不会尝试解析[[未解析函数]]。
   * 它只根据函数标识符执行简单的存在性检查，以快速识别未定义的函数，
   * 而不触发关系解析，这在某些情况下可能会导致昂贵的分区/模式发现过程。
   * 为了避免重复的外部函数查找，外部函数标识符将存储在本地哈希集ExternalFunctionNameset中。
   * 
   * @see [[ResolveFunctions]]
   * @see https://issues.apache.org/jira/browse/SPARK-19737
   */
  object LookupFunctions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      //外部函数标识符
      val externalFunctionNameSet = new mutable.HashSet[FunctionIdentifier]()
      plan.resolveExpressions {
        case f: UnresolvedFunction
          if externalFunctionNameSet.contains(normalizeFuncName(f.name)) => f
        case f: UnresolvedFunction if catalog.isRegisteredFunction(f.name) => f
        //如果未解析的函数是持久化，则新增到externalFunctinsNameSet
        case f: UnresolvedFunction if catalog.isPersistentFunction(f.name) =>
          externalFunctionNameSet.add(normalizeFuncName(f.name))
          f
        case f: UnresolvedFunction =>
          withPosition(f) {
            //找不到此方法
            throw new NoSuchFunctionException(f.name.database.getOrElse(catalog.getCurrentDatabase),
              f.name.funcName)
          }
      }
    }

    def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
      val funcName = if (conf.caseSensitiveAnalysis) {
        name.funcName
      } else {
        name.funcName.toLowerCase(Locale.ROOT)
      }

      val databaseName = name.database match {
        case Some(a) => formatDatabaseName(a)
        case None => catalog.getCurrentDatabase
      }

      FunctionIdentifier(funcName, Some(databaseName))
    }

    protected def formatDatabaseName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }
  }

  /**
   * 把 [[UnresolvedFunction]]s 转换为具体的[[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // 跳过，直到子节点解析
          case u: UnresolvedAttribute if resolver(u.name, VirtualColumn.hiveGroupingIdName) =>
            withPosition(u) {
              Alias(GroupingID(Nil), VirtualColumn.hiveGroupingIdName)()
            }
          case u @ UnresolvedGenerator(name, children) =>
            withPosition(u) {
              catalog.lookupFunction(name, children) match {
                case generator: Generator => generator
                case other =>
                  failAnalysis(s"$name is expected to be a generator. However, " +
                    s"its class is ${other.getClass.getCanonicalName}, which is not a generator.")
              }
            }
          case u @ UnresolvedFunction(funcId, children, isDistinct) =>
            withPosition(u) {
              catalog.lookupFunction(funcId, children) match {
                //AggregateWindowFunctions是只能在
                //window子句的上下文。它们不需要用
                //聚合表达式。
                case wf: AggregateWindowFunction =>
                  if (isDistinct) {
                    failAnalysis(s"${wf.prettyName} does not support the modifier DISTINCT")
                  } else {
                    wf
                  }
                // 我们得到一个聚合函数，我们需要将它包装在一个AggregateExpression中。
                case agg: AggregateFunction => AggregateExpression(agg, Complete, isDistinct)
                // 此函数不是聚合函数，只返回已解析的函数。
                case other =>
                  if (isDistinct) {
                    failAnalysis(s"${other.prettyName} does not support the modifier DISTINCT")
                  } else {
                    other
                  }
              }
            }
        }
    }
  }

  /**
   * 此规则解析并重写表达式中的子查询。
   *
   * 注：CTE在CTESubstitution替换中处理。
   */
  object ResolveSubquery extends Rule[LogicalPlan] with PredicateHelper {
    /**
     * 通过使用外部计划的引用解析子查询中的相关表达式。
     * 全部解析的外部引用被包装在一个[[outerreference]]
     */
    private def resolveOuterReferences(plan: LogicalPlan, outer: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsDown {
        case q: LogicalPlan if q.childrenResolved && !q.resolved =>
          q transformExpressions {
            case u @ UnresolvedAttribute(nameParts) =>
              withPosition(u) {
                try {
                  outer.resolve(nameParts, resolver) match {
                    case Some(outerAttr) => OuterReference(outerAttr)
                    case None => u
                  }
                } catch {
                  case _: AnalysisException => u
                }
              }
          }
      }
    }

    /**
     * 解析子查询表达式中引用的子查询计划。
     * 
     * 常规属性引用使用常规分析器解析，外部引用使用ResolveOuterReferences方法从外部计划解析。
     *
     * 相关谓词的外部引用将更新为子查询表达式的子级。
     * 
     */
    private def resolveSubQuery(
        e: SubqueryExpression,
        plans: Seq[LogicalPlan])(
        f: (LogicalPlan, Seq[Expression]) => SubqueryExpression): SubqueryExpression = {
      // Step 1: 解析外部表达式。
      var previous: LogicalPlan = null
      var current = e.plan
      do {
        // 尝试使用常规分析器解析子查询计划。
        previous = current
        current = executeSameContext(current)

        // 如果尚未解析子查询计划，请使用外部引用来解析它。
        val i = plans.iterator
        val afterResolve = current
        while (!current.resolved && current.fastEquals(afterResolve) && i.hasNext) {
          current = resolveOuterReferences(current, i.next())
        }
      } while (!current.resolved && !current.fastEquals(previous))

      // Step 2: 如果子查询计划已完全解析，则拉动外部引用并将其记录为子查询表达式的子级。
      if (current.resolved) {
        // 将外部引用记录为子查询表达式的子级。
        f(current, SubExprUtils.getOuterReferences(current))
      } else {
        e.withNewPlan(current)
      }
    }

    /**
     * .
     * 除了解析子查询计划中的子查询和外部引用（如果有的话），子查询表达式的子级也会更新以记录外部引用。
     * 需要两点
     * (1) 在优化期间，不会从计划中删除来自外部查询的列   
     * (2) 引用外部属性的任何聚合表达式都被下推到外部计划以进行计算。
     *     
     */
    private def resolveSubQueries(plan: LogicalPlan, plans: Seq[LogicalPlan]): LogicalPlan = {
      plan transformExpressions {
        case s @ ScalarSubquery(sub, _, exprId) if !sub.resolved =>
          resolveSubQuery(s, plans)(ScalarSubquery(_, _, exprId))
        case e @ Exists(sub, _, exprId) if !sub.resolved =>
          resolveSubQuery(e, plans)(Exists(_, _, exprId))
        case InSubquery(values, l @ ListQuery(_, _, exprId, _))
            if values.forall(_.resolved) && !l.resolved =>
          val expr = resolveSubQuery(l, plans)((plan, exprs) => {
            ListQuery(plan, exprs, exprId, plan.output)
          })
          InSubquery(values, expr.asInstanceOf[ListQuery])
      }
    }

    /**
     * Resolve and rewrite all subqueries in an operator tree..
     */
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // In case of HAVING (a filter after an aggregate) we use both the aggregate and
      // its child for resolution.
      case f @ Filter(_, a: Aggregate) if f.childrenResolved =>
        resolveSubQueries(f, Seq(a, a.child))
      // Only a few unary nodes (Project/Filter/Aggregate) can contain subqueries.
      case q: UnaryNode if q.childrenResolved =>
        resolveSubQueries(q, q.children)
    }
  }

  /**
   * 用projections替换子查询的未解析列别名。
   */
  object ResolveSubqueryColumnAliases extends Rule[LogicalPlan] {

     def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u @ UnresolvedSubqueryColumnAliases(columnNames, child) if child.resolved =>
        // 如果查询的子查询中有别名，则解析输出属性：
        // e.g., SELECT * FROM (SELECT 1 AS a, 1 AS b) t(col1, col2)
        val outputAttrs = child.output
        // 检查别名数是否等于子查询中的输出列数。
        if (columnNames.size != outputAttrs.size) {
          u.failAnalysis("Number of column aliases does not match number of columns. " +
            s"Number of column aliases: ${columnNames.size}; " +
            s"number of columns: ${outputAttrs.size}.")
        }
        val aliases = outputAttrs.zip(columnNames).map { case (attr, aliasName) =>
          Alias(attr, aliasName)()
        }
        Project(aliases, child)
    }
  }

  /**
   * 把projections中包含聚合表达式的转为聚合
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      // 收集所有窗口聚合表达式。
      val windowedAggExprs = exprs.flatMap { expr =>
        expr.collect {
          case WindowExpression(ae: AggregateExpression, _) => ae
        }
      }.toSet

      //查找第一个没有窗口的聚合表达式。
      exprs.exists(_.collectFirst {
        case ae: AggregateExpression if !windowedAggExprs.contains(ae) => ae
      }.isDefined)
    }
  }

  /**
   * 此规则查找不在聚合运算符中的聚合表达式。 例如
   * HAVING子句或ORDER BY子句中的那些.  
   * 这些表达式被下推到基础聚合运算符，然后在原始运算符之后被投影掉。
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case f @ Filter(cond, agg @ Aggregate(grouping, originalAggExprs, child)) if agg.resolved =>

        // 尝试像在aggregate子句中那样解析筛选器的条件。
        try {
          val aggregatedCondition =
            Aggregate(
              grouping,
              Alias(cond, "havingCondition")() :: Nil,
              child)
          val resolvedOperator = executeSameContext(aggregatedCondition)
          def resolvedAggregateFilter =
            resolvedOperator
              .asInstanceOf[Aggregate]
              .aggregateExpressions.head

          // 如果解析成功，并且我们看到过滤器中有一个聚合，请将其添加到原始聚合运算符中。
          if (resolvedOperator.resolved) {
            // 尝试用别名替换筛选器中的所有聚合表达式。
            val aggregateExpressions = ArrayBuffer.empty[NamedExpression]
            val transformedAggregateFilter = resolvedAggregateFilter.transform {
              case ae: AggregateExpression =>
                val alias = Alias(ae, ae.toString)()
                aggregateExpressions += alias
                alias.toAttribute
              // 分组函数在规则中处理[[ResolveGroupingAnalytics]].
              case e: Expression if grouping.exists(_.semanticEquals(e)) &&
                  !ResolveGroupingAnalytics.hasGroupingFunction(e) &&
                  !agg.output.exists(_.semanticEquals(e)) =>
                e match {
                  case ne: NamedExpression =>
                    aggregateExpressions += ne
                    ne.toAttribute
                  case _ =>
                    val alias = Alias(e, e.toString)()
                    aggregateExpressions += alias
                    alias.toAttribute
                }
            }

            // 将聚合表达式推送到聚合中（如果有）。
            if (aggregateExpressions.nonEmpty) {
              Project(agg.output,
                Filter(transformedAggregateFilter,
                  agg.copy(aggregateExpressions = originalAggExprs ++ aggregateExpressions)))
            } else {
              f
            }
          } else {
            f
          }
        } catch {
          // 尝试在聚合中解析可能会导致歧义。  
          // 如果有歧义就返回原始
          case ae: AnalysisException => f
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate) if aggregate.resolved =>

        // 尝试像在aggregate子句中那样解析排序。
        try {
          // 如果排序顺序未解析、包含不在聚合中的引用或包含“aggregateExpression”，则需要将其下推到基础聚合运算符。
          val unresolvedSortOrders = sortOrder.filter { s =>
            !s.resolved || !s.references.subsetOf(aggregate.outputSet) || containsAggregate(s)
          }
          val aliasedOrdering =
            unresolvedSortOrders.map(o => Alias(o.child, "aggOrder")())
          val aggregatedOrdering = aggregate.copy(aggregateExpressions = aliasedOrdering)
          val resolvedAggregate: Aggregate =
            executeSameContext(aggregatedOrdering).asInstanceOf[Aggregate]
          val resolvedAliasedOrdering: Seq[Alias] =
            resolvedAggregate.aggregateExpressions.asInstanceOf[Seq[Alias]]

          // 如果我们通过分析检查，那么排序表达式应该只引用聚合表达式或分组表达式，并且可以安全地将它们下推到聚合。
          checkAnalysis(resolvedAggregate)

          val originalAggExprs = aggregate.aggregateExpressions.map(
            CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])

          //如果排序表达式与原始聚合表达式相同，则不需要
          //下推此排序表达式并可以引用原始聚合
          //改为表达式。
          val needsPushDown = ArrayBuffer.empty[NamedExpression]
          val evaluatedOrderings = resolvedAliasedOrdering.zip(unresolvedSortOrders).map {
            case (evaluated, order) =>
              val index = originalAggExprs.indexWhere {
                case Alias(child, _) => child semanticEquals evaluated.child
                case other => other semanticEquals evaluated.child
              }

              if (index == -1) {
                needsPushDown += evaluated
                order.copy(child = evaluated.toAttribute)
              } else {
                order.copy(child = originalAggExprs(index).toAttribute)
              }
          }

          val sortOrdersMap = unresolvedSortOrders
            .map(new TreeNodeRef(_))
            .zip(evaluatedOrderings)
            .toMap
          val finalSortOrders = sortOrder.map(s => sortOrdersMap.getOrElse(new TreeNodeRef(s), s))

          //因为我们不依赖sort.resolved作为此规则的停止条件，
          //我们需要检查并防止多次应用此规则
          if (sortOrder == finalSortOrders) {
            sort
          } else {
            Project(aggregate.output,
              Sort(finalSortOrders, global,
                aggregate.copy(aggregateExpressions = originalAggExprs ++ needsPushDown)))
          }
        } catch {
          //尝试在聚合中解析可能会导致歧义。当这种情况发生时
          //只需返回原始计划即可。
          case ae: AnalysisException => sort
        }
    }

    def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Extracts [[Generator]] from the projectList of a [[Project]] operator and creates [[Generate]]
   * operator under [[Project]].
   *
   * This rule will throw [[AnalysisException]] for following cases:
   * 1. [[Generator]] is nested in expressions, e.g. `SELECT explode(list) + 1 FROM tbl`
   * 2. more than one [[Generator]] is found in projectList,
   *    e.g. `SELECT explode(list), explode(list) FROM tbl`
   * 3. [[Generator]] is found in other operators that are not [[Project]] or [[Generate]],
   *    e.g. `SELECT * FROM tbl SORT BY explode(list)`
   */
  object ExtractGenerator extends Rule[LogicalPlan] {
    private def hasGenerator(expr: Expression): Boolean = {
      expr.find(_.isInstanceOf[Generator]).isDefined
    }

    private def hasNestedGenerator(expr: NamedExpression): Boolean = {
      CleanupAliases.trimNonTopLevelAliases(expr) match {
        case UnresolvedAlias(_: Generator, _) => false
        case Alias(_: Generator, _) => false
        case MultiAlias(_: Generator, _) => false
        case other => hasGenerator(other)
      }
    }

    private def trimAlias(expr: NamedExpression): Expression = expr match {
      case UnresolvedAlias(child, _) => child
      case Alias(child, _) => child
      case MultiAlias(child, _) => child
      case _ => expr
    }

    private object AliasedGenerator {
      /**
       * Extracts a [[Generator]] expression, any names assigned by aliases to the outputs
       * and the outer flag. The outer flag is used when joining the generator output.
       * @param e the [[Expression]]
       * @return (the [[Generator]], seq of output names, outer flag)
       */
      def unapply(e: Expression): Option[(Generator, Seq[String], Boolean)] = e match {
        case Alias(GeneratorOuter(g: Generator), name) if g.resolved => Some((g, name :: Nil, true))
        case MultiAlias(GeneratorOuter(g: Generator), names) if g.resolved => Some((g, names, true))
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil, false))
        case MultiAlias(g: Generator, names) if g.resolved => Some((g, names, false))
        case _ => None
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Project(projectList, _) if projectList.exists(hasNestedGenerator) =>
        val nestedGenerator = projectList.find(hasNestedGenerator).get
        throw new AnalysisException("Generators are not supported when it's nested in " +
          "expressions, but got: " + toPrettySQL(trimAlias(nestedGenerator)))

      case Project(projectList, _) if projectList.count(hasGenerator) > 1 =>
        val generators = projectList.filter(hasGenerator).map(trimAlias)
        throw new AnalysisException("Only one generator allowed per select clause but found " +
          generators.size + ": " + generators.map(toPrettySQL).mkString(", "))

      case p @ Project(projectList, child) =>
        // Holds the resolved generator, if one exists in the project list.
        var resolvedGenerator: Generate = null

        val newProjectList = projectList
          .map(CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
          .flatMap {
            case AliasedGenerator(generator, names, outer) if generator.childrenResolved =>
              // It's a sanity check, this should not happen as the previous case will throw
              // exception earlier.
              assert(resolvedGenerator == null, "More than one generator found in SELECT.")

              resolvedGenerator =
                Generate(
                  generator,
                  unrequiredChildIndex = Nil,
                  outer = outer,
                  qualifier = None,
                  generatorOutput = ResolveGenerate.makeGeneratorOutput(generator, names),
                  child)

              resolvedGenerator.generatorOutput
            case other => other :: Nil
          }

        if (resolvedGenerator != null) {
          Project(newProjectList, resolvedGenerator)
        } else {
          p
        }

      case g: Generate => g

      case p if p.expressions.exists(hasGenerator) =>
        throw new AnalysisException("Generators are not supported outside the SELECT clause, but " +
          "got: " + p.simpleString(SQLConf.get.maxToStringFields))
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
   *  - concrete attribute references for their output.
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]].
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case g: Generate if !g.child.resolved || !g.generator.resolved => g
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned from field names in generator.
     */
    private[analysis] def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementAttrs = generator.elementSchema.toAttributes

      if (names.length == elementAttrs.length) {
        names.zip(elementAttrs).map {
          case (name, attr) => attr.withName(name)
        }
      } else if (names.isEmpty) {
        elementAttrs
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementAttrs.size} aliases but got " +
          s"${names.mkString(",")} ")
      }
    }
  }

  /**
   * Extracts [[WindowExpression]]s from the projectList of a [[Project]] operator and
   * aggregateExpressions of an [[Aggregate]] operator and creates individual [[Window]]
   * operators for every distinct [[WindowSpecDefinition]].
   *
   * This rule handles three cases:
   *  - A [[Project]] having [[WindowExpression]]s in its projectList;
   *  - An [[Aggregate]] having [[WindowExpression]]s in its aggregateExpressions.
   *  - A [[Filter]]->[[Aggregate]] pattern representing GROUP BY with a HAVING
   *    clause and the [[Aggregate]] has [[WindowExpression]]s in its aggregateExpressions.
   * Note: If there is a GROUP BY clause in the query, aggregations and corresponding
   * filters (expressions in the HAVING clause) should be evaluated before any
   * [[WindowExpression]]. If a query has SELECT DISTINCT, the DISTINCT part should be
   * evaluated after all [[WindowExpression]]s.
   *
   * For every case, the transformation works as follows:
   * 1. For a list of [[Expression]]s (a projectList or an aggregateExpressions), partitions
   *    it two lists of [[Expression]]s, one for all [[WindowExpression]]s and another for
   *    all regular expressions.
   * 2. For all [[WindowExpression]]s, groups them based on their [[WindowSpecDefinition]]s
   *    and [[WindowFunctionType]]s.
   * 3. For every distinct [[WindowSpecDefinition]] and [[WindowFunctionType]], creates a
   *    [[Window]] operator and inserts it into the plan tree.
   */
  object ExtractWindowExpressions extends Rule[LogicalPlan] {
    private def hasWindowFunction(exprs: Seq[Expression]): Boolean =
      exprs.exists(hasWindowFunction)

    private def hasWindowFunction(expr: Expression): Boolean = {
      expr.find {
        case window: WindowExpression => true
        case _ => false
      }.isDefined
    }

    /**
     * From a Seq of [[NamedExpression]]s, extract expressions containing window expressions and
     * other regular expressions that do not contain any window expression. For example, for
     * `col1, Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5)`, we will extract
     * `col1`, `col2 + col3`, `col4`, and `col5` out and replace their appearances in
     * the window expression as attribute references. So, the first returned value will be
     * `[Sum(_w0) OVER (PARTITION BY _w1 ORDER BY _w2)]` and the second returned value will be
     * [col1, col2 + col3 as _w0, col4 as _w1, col5 as _w2].
     *
     * @return (seq of expressions containing at least one window expression,
     *          seq of non-window expressions)
     */
    private def extract(
        expressions: Seq[NamedExpression]): (Seq[NamedExpression], Seq[NamedExpression]) = {
      // First, we partition the input expressions to two part. For the first part,
      // every expression in it contain at least one WindowExpression.
      // Expressions in the second part do not have any WindowExpression.
      val (expressionsWithWindowFunctions, regularExpressions) =
        expressions.partition(hasWindowFunction)

      // Then, we need to extract those regular expressions used in the WindowExpression.
      // For example, when we have col1 - Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5),
      // we need to make sure that col1 to col5 are all projected from the child of the Window
      // operator.
      val extractedExprBuffer = new ArrayBuffer[NamedExpression]()
      def extractExpr(expr: Expression): Expression = expr match {
        case ne: NamedExpression =>
          // If a named expression is not in regularExpressions, add it to
          // extractedExprBuffer and replace it with an AttributeReference.
          val missingExpr =
            AttributeSet(Seq(expr)) -- (regularExpressions ++ extractedExprBuffer)
          if (missingExpr.nonEmpty) {
            extractedExprBuffer += ne
          }
          // alias will be cleaned in the rule CleanupAliases
          ne
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an internal column name, e.g. "_w0").
          val withName = Alias(e, s"_w${extractedExprBuffer.length}")()
          extractedExprBuffer += withName
          withName.toAttribute
      }

      // Now, we extract regular expressions from expressionsWithWindowFunctions
      // by using extractExpr.
      val seenWindowAggregates = new ArrayBuffer[AggregateExpression]
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        _.transform {
          // Extracts children expressions of a WindowFunction (input parameters of
          // a WindowFunction).
          case wf: WindowFunction =>
            val newChildren = wf.children.map(extractExpr)
            wf.withNewChildren(newChildren)

          // Extracts expressions from the partition spec and order spec.
          case wsc @ WindowSpecDefinition(partitionSpec, orderSpec, _) =>
            val newPartitionSpec = partitionSpec.map(extractExpr)
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractExpr(so.child)
              so.copy(child = newChild)
            }
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)

          // Extract Windowed AggregateExpression
          case we @ WindowExpression(
              ae @ AggregateExpression(function, _, _, _),
              spec: WindowSpecDefinition) =>
            val newChildren = function.children.map(extractExpr)
            val newFunction = function.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
            val newAgg = ae.copy(aggregateFunction = newFunction)
            seenWindowAggregates += newAgg
            WindowExpression(newAgg, spec)

          case AggregateExpression(aggFunc, _, _, _) if hasWindowFunction(aggFunc.children) =>
            failAnalysis("It is not allowed to use a window function inside an aggregate " +
              "function. Please use the inner window function in a sub-query.")

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression if !seenWindowAggregates.contains(agg) =>
            val withName = Alias(agg, s"_w${extractedExprBuffer.length}")()
            extractedExprBuffer += withName
            withName.toAttribute

          // Extracts other attributes
          case attr: Attribute => extractExpr(attr)

        }.asInstanceOf[NamedExpression]
      }

      (newExpressionsWithWindowFunctions, regularExpressions ++ extractedExprBuffer)
    } // end of extract

    /**
     * Adds operators for Window Expressions. Every Window operator handles a single Window Spec.
     */
    private def addWindow(
        expressionsWithWindowFunctions: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      // First, we need to extract all WindowExpressions from expressionsWithWindowFunctions
      // and put those extracted WindowExpressions to extractedWindowExprBuffer.
      // This step is needed because it is possible that an expression contains multiple
      // WindowExpressions with different Window Specs.
      // After extracting WindowExpressions, we need to construct a project list to generate
      // expressionsWithWindowFunctions based on extractedWindowExprBuffer.
      // For example, for "sum(a) over (...) / sum(b) over (...)", we will first extract
      // "sum(a) over (...)" and "sum(b) over (...)" out, and assign "_we0" as the alias to
      // "sum(a) over (...)" and "_we1" as the alias to "sum(b) over (...)".
      // Then, the projectList will be [_we0/_we1].
      val extractedWindowExprBuffer = new ArrayBuffer[NamedExpression]()
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        // We need to use transformDown because we want to trigger
        // "case alias @ Alias(window: WindowExpression, _)" first.
        _.transformDown {
          case alias @ Alias(window: WindowExpression, _) =>
            // If a WindowExpression has an assigned alias, just use it.
            extractedWindowExprBuffer += alias
            alias.toAttribute
          case window: WindowExpression =>
            // If there is no alias assigned to the WindowExpressions. We create an
            // internal column.
            val withName = Alias(window, s"_we${extractedWindowExprBuffer.length}")()
            extractedWindowExprBuffer += withName
            withName.toAttribute
        }.asInstanceOf[NamedExpression]
      }

      // Second, we group extractedWindowExprBuffer based on their Partition and Order Specs.
      val groupedWindowExpressions = extractedWindowExprBuffer.groupBy { expr =>
        val distinctWindowSpec = expr.collect {
          case window: WindowExpression => window.windowSpec
        }.distinct

        // We do a final check and see if we only have a single Window Spec defined in an
        // expressions.
        if (distinctWindowSpec.isEmpty) {
          failAnalysis(s"$expr does not have any WindowExpression.")
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          failAnalysis(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
            s"Please file a bug report with this error message, stack trace, and the query.")
        } else {
          val spec = distinctWindowSpec.head
          (spec.partitionSpec, spec.orderSpec, WindowFunctionType.functionType(expr))
        }
      }.toSeq

      // Third, we aggregate them by adding each Window operator for each Window Spec and then
      // setting this to the child of the next Window operator.
      val windowOps =
        groupedWindowExpressions.foldLeft(child) {
          case (last, ((partitionSpec, orderSpec, _), windowExpressions)) =>
            Window(windowExpressions, partitionSpec, orderSpec, last)
        }

      // Finally, we create a Project to output windowOps's output
      // newExpressionsWithWindowFunctions.
      Project(windowOps.output ++ newExpressionsWithWindowFunctions, windowOps)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {

      case Filter(condition, _) if hasWindowFunction(condition) =>
        failAnalysis("It is not allowed to use window functions inside WHERE and HAVING clauses")

      // Aggregate with Having clause. This rule works with an unresolved Aggregate because
      // a resolved Aggregate will not have Window Functions.
      case f @ Filter(condition, a @ Aggregate(groupingExprs, aggregateExprs, child))
        if child.resolved &&
           hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add a Filter operator for conditions in the Having clause.
        val withFilter = Filter(condition, withAggregate)
        val withWindow = addWindow(windowExpressions, withFilter)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map(_.toAttribute)
        Project(finalProjectList, withWindow)

      case p: LogicalPlan if !p.childrenResolved => p

      // Aggregate without Having clause.
      case a @ Aggregate(groupingExprs, aggregateExprs, child)
        if hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withAggregate)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map(_.toAttribute)
        Project(finalProjectList, withWindow)

      // We only extract Window Expressions after all expressions of the Project
      // have been resolved.
      case p @ Project(projectList, child)
        if hasWindowFunction(projectList) && !p.expressions.exists(!_.resolved) =>
        val (windowExpressions, regularExpressions) = extract(projectList)
        // We add a project to get all needed expressions for window expressions from the child
        // of the original Project operator.
        val withProject = Project(regularExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withProject)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = projectList.map(_.toAttribute)
        Project(finalProjectList, withWindow)
    }
  }

  /**
   * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
   * put them into an inner Project and finally project them away at the outer Project.
   */
  object PullOutNondeterministic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: Project => p
      case f: Filter => f

      case a: Aggregate if a.groupingExpressions.exists(!_.deterministic) =>
        val nondeterToAttr = getNondeterToAttr(a.groupingExpressions)
        val newChild = Project(a.child.output ++ nondeterToAttr.values, a.child)
        a.transformExpressions { case e =>
          nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
        }.copy(child = newChild)

      // todo: It's hard to write a general rule to pull out nondeterministic expressions
      // from LogicalPlan, currently we only do it for UnaryNode which has same output
      // schema with its child.
      case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
        val nondeterToAttr = getNondeterToAttr(p.expressions)
        val newPlan = p.transformExpressions { case e =>
          nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
        }
        val newChild = Project(p.child.output ++ nondeterToAttr.values, p.child)
        Project(p.output, newPlan.withNewChildren(newChild :: Nil))
    }

    private def getNondeterToAttr(exprs: Seq[Expression]): Map[Expression, NamedExpression] = {
      exprs.filterNot(_.deterministic).flatMap { expr =>
        val leafNondeterministic = expr.collect { case n: Nondeterministic => n }
        leafNondeterministic.distinct.map { e =>
          val ne = e match {
            case n: NamedExpression => n
            case _ => Alias(e, "_nondeterministic")()
          }
          e -> ne
        }
      }.toMap
    }
  }

  /**
   * Set the seed for random number generation.
   */
  object ResolveRandomSeed extends Rule[LogicalPlan] {
    private lazy val random = new Random()

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if p.resolved => p
      case p => p transformExpressionsUp {
        case Uuid(None) => Uuid(Some(random.nextLong()))
        case Shuffle(child, None) => Shuffle(child, Some(random.nextLong()))
      }
    }
  }

  /**
   * Correctly handle null primitive inputs for UDF by adding extra [[If]] expression to do the
   * null check.  When user defines a UDF with primitive parameters, there is no way to tell if the
   * primitive parameter is null or not, so here we assume the primitive input is null-propagatable
   * and we should return null if the input is null.
   */
  object HandleNullInputsForUDF extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.resolved => p // Skip unresolved nodes.

      case p => p transformExpressionsUp {

        case udf @ ScalaUDF(_, _, inputs, inputPrimitives, _, _, _, _)
            if inputPrimitives.contains(true) =>
          // Otherwise, add special handling of null for fields that can't accept null.
          // The result of operations like this, when passed null, is generally to return null.
          assert(inputPrimitives.length == inputs.length)

          val inputPrimitivesPair = inputPrimitives.zip(inputs)
          val inputNullCheck = inputPrimitivesPair.collect {
            case (isPrimitive, input) if isPrimitive && input.nullable =>
              IsNull(input)
          }.reduceLeftOption[Expression](Or)

          if (inputNullCheck.isDefined) {
            // Once we add an `If` check above the udf, it is safe to mark those checked inputs
            // as null-safe (i.e., wrap with `KnownNotNull`), because the null-returning
            // branch of `If` will be called if any of these checked inputs is null. Thus we can
            // prevent this rule from being applied repeatedly.
            val newInputs = inputPrimitivesPair.map {
              case (isPrimitive, input) =>
                if (isPrimitive && input.nullable) {
                  KnownNotNull(input)
                } else {
                  input
                }
            }
            val newUDF = udf.copy(children = newInputs)
            If(inputNullCheck.get, Literal.create(null, udf.dataType), newUDF)
          } else {
            udf
          }
      }
    }
  }

  /**
   * Check and add proper window frames for all window functions.
   */
  object ResolveWindowFrame extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
          if wf.frame != UnspecifiedFrame && wf.frame != f =>
        failAnalysis(s"Window Frame $f must match the required frame ${wf.frame}")
      case WindowExpression(wf: WindowFunction, s @ WindowSpecDefinition(_, _, UnspecifiedFrame))
          if wf.frame != UnspecifiedFrame =>
        WindowExpression(wf, s.copy(frameSpecification = wf.frame))
      case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame))
          if e.resolved =>
        val frame = if (o.nonEmpty) {
          SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
        } else {
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
        }
        we.copy(windowSpec = s.copy(frameSpecification = frame))
    }
  }

  /**
   * Check and add order to [[AggregateWindowFunction]]s.
   */
  object ResolveWindowOrder extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
        failAnalysis(s"Window function $wf requires window to be ordered, please add ORDER BY " +
          s"clause. For example SELECT $wf(value_expr) OVER (PARTITION BY window_partition " +
          s"ORDER BY window_ordering) from table")
      case WindowExpression(rank: RankLike, spec) if spec.resolved =>
        val order = spec.orderSpec.map(_.child)
        WindowExpression(rank.withOrder(order), spec)
    }
  }

  /**
   * Removes natural or using joins by calculating output columns based on output from two sides,
   * Then apply a Project on a normal Join to eliminate natural or using join.
   */
  object ResolveNaturalAndUsingJoin extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case j @ Join(left, right, UsingJoin(joinType, usingCols), _, hint)
          if left.resolved && right.resolved && j.duplicateResolved =>
        commonNaturalJoinProcessing(left, right, joinType, usingCols, None, hint)
      case j @ Join(left, right, NaturalJoin(joinType), condition, hint)
          if j.resolvedExceptNatural =>
        // find common column names from both sides
        val joinNames = left.output.map(_.name).intersect(right.output.map(_.name))
        commonNaturalJoinProcessing(left, right, joinType, joinNames, condition, hint)
    }
  }

  /**
   * Resolves columns of an output table from the data in a logical plan. This rule will:
   *
   * - Reorder columns when the write is by name
   * - Insert safe casts when data types do not match
   * - Insert aliases when column names do not match
   * - Detect plans that are not compatible with the output table and throw AnalysisException
   */
  object ResolveOutputRelation extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case append @ AppendData(table, query, isByName)
          if table.resolved && query.resolved && !append.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          append.copy(query = projection)
        } else {
          append
        }

      case overwrite @ OverwriteByExpression(table, _, query, isByName)
          if table.resolved && query.resolved && !overwrite.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          overwrite.copy(query = projection)
        } else {
          overwrite
        }

      case overwrite @ OverwritePartitionsDynamic(table, query, isByName)
          if table.resolved && query.resolved && !overwrite.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          overwrite.copy(query = projection)
        } else {
          overwrite
        }
    }

    def resolveOutputColumns(
        tableName: String,
        expected: Seq[Attribute],
        query: LogicalPlan,
        byName: Boolean): LogicalPlan = {

      if (expected.size < query.output.size) {
        throw new AnalysisException(
          s"""Cannot write to '$tableName', too many data columns:
             |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
             |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}""".stripMargin)
      }

      val errors = new mutable.ArrayBuffer[String]()
      val resolved: Seq[NamedExpression] = if (byName) {
        expected.flatMap { tableAttr =>
          query.resolveQuoted(tableAttr.name, resolver) match {
            case Some(queryExpr) =>
              checkField(tableAttr, queryExpr, byName, err => errors += err)
            case None =>
              errors += s"Cannot find data for output column '${tableAttr.name}'"
              None
          }
        }

      } else {
        if (expected.size > query.output.size) {
          throw new AnalysisException(
            s"""Cannot write to '$tableName', not enough data columns:
               |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
               |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}"""
                .stripMargin)
        }

        query.output.zip(expected).flatMap {
          case (queryExpr, tableAttr) =>
            checkField(tableAttr, queryExpr, byName, err => errors += err)
        }
      }

      if (errors.nonEmpty) {
        throw new AnalysisException(
          s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
      }

      Project(resolved, query)
    }

    private def checkField(
        tableAttr: Attribute,
        queryExpr: NamedExpression,
        byName: Boolean,
        addError: String => Unit): Option[NamedExpression] = {

      // run the type check first to ensure type errors are present
      val canWrite = DataType.canWrite(
        queryExpr.dataType, tableAttr.dataType, byName, resolver, tableAttr.name, addError)

      if (queryExpr.nullable && !tableAttr.nullable) {
        addError(s"Cannot write nullable values to non-null column '${tableAttr.name}'")
        None

      } else if (!canWrite) {
        None

      } else {
        // always add an UpCast. it will be removed in the optimizer if it is unnecessary.
        Some(Alias(
          UpCast(queryExpr, tableAttr.dataType), tableAttr.name
        )(
          explicitMetadata = Option(tableAttr.metadata)
        ))
      }
    }
  }

  private def commonNaturalJoinProcessing(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression],
      hint: JoinHint) = {
    val leftKeys = joinNames.map { keyName =>
      left.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the left " +
          s"side of the join. The left-side columns: [${left.output.map(_.name).mkString(", ")}]")
      }
    }
    val rightKeys = joinNames.map { keyName =>
      right.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the right " +
          s"side of the join. The right-side columns: [${right.output.map(_.name).mkString(", ")}]")
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)

    val newCondition = (condition ++ joinPairs.map(EqualTo.tupled)).reduceOption(And)

    // columns not in joinPairs
    val lUniqueOutput = left.output.filterNot(att => leftKeys.contains(att))
    val rUniqueOutput = right.output.filterNot(att => rightKeys.contains(att))

    // the output list looks like: join keys, columns from left, columns from right
    val projectList = joinType match {
      case LeftOuter =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true))
      case LeftExistence(_) =>
        leftKeys ++ lUniqueOutput
      case RightOuter =>
        rightKeys ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput
      case FullOuter =>
        // in full outer join, joinCols should be non-null if there is.
        val joinedCols = joinPairs.map { case (l, r) => Alias(Coalesce(Seq(l, r)), l.name)() }
        joinedCols ++
          lUniqueOutput.map(_.withNullability(true)) ++
          rUniqueOutput.map(_.withNullability(true))
      case _ : InnerLike =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput
      case _ =>
        sys.error("Unsupported natural join type " + joinType)
    }
    // use Project to trim unnecessary fields
    Project(projectList, Join(left, right, joinType, newCondition, hint))
  }

  /**
   * Replaces [[UnresolvedDeserializer]] with the deserialization expression that has been resolved
   * to the given input attributes.
   */
  object ResolveDeserializer extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case UnresolvedDeserializer(deserializer, inputAttributes) =>
          val inputs = if (inputAttributes.isEmpty) {
            p.children.flatMap(_.output)
          } else {
            inputAttributes
          }

          validateTopLevelTupleFields(deserializer, inputs)
          val resolved = resolveExpressionBottomUp(
            deserializer, LocalRelation(inputs), throws = true)
          val result = resolved transformDown {
            case UnresolvedMapObjects(func, inputData, cls) if inputData.resolved =>
              inputData.dataType match {
                case ArrayType(et, cn) =>
                  MapObjects(func, inputData, et, cn, cls) transformUp {
                    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
                      ExtractValue(child, fieldName, resolver)
                  }
                case other =>
                  throw new AnalysisException("need an array field but got " + other.catalogString)
              }
            case u: UnresolvedCatalystToExternalMap if u.child.resolved =>
              u.child.dataType match {
                case _: MapType =>
                  CatalystToExternalMap(u) transformUp {
                    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
                      ExtractValue(child, fieldName, resolver)
                  }
                case other =>
                  throw new AnalysisException("need a map field but got " + other.catalogString)
              }
          }
          validateNestedTupleFields(result)
          result
      }
    }

    private def fail(schema: StructType, maxOrdinal: Int): Unit = {
      throw new AnalysisException(s"Try to map ${schema.catalogString} to Tuple${maxOrdinal + 1}" +
        ", but failed as the number of fields does not line up.")
    }

    /**
     * For each top-level Tuple field, we use [[GetColumnByOrdinal]] to get its corresponding column
     * by position.  However, the actual number of columns may be different from the number of Tuple
     * fields.  This method is used to check the number of columns and fields, and throw an
     * exception if they do not match.
     */
    private def validateTopLevelTupleFields(
        deserializer: Expression, inputs: Seq[Attribute]): Unit = {
      val ordinals = deserializer.collect {
        case GetColumnByOrdinal(ordinal, _) => ordinal
      }.distinct.sorted

      if (ordinals.nonEmpty && ordinals != inputs.indices) {
        fail(inputs.toStructType, ordinals.last)
      }
    }

    /**
     * For each nested Tuple field, we use [[GetStructField]] to get its corresponding struct field
     * by position.  However, the actual number of struct fields may be different from the number
     * of nested Tuple fields.  This method is used to check the number of struct fields and nested
     * Tuple fields, and throw an exception if they do not match.
     */
    private def validateNestedTupleFields(deserializer: Expression): Unit = {
      val structChildToOrdinals = deserializer
        // There are 2 kinds of `GetStructField`:
        //   1. resolved from `UnresolvedExtractValue`, and it will have a `name` property.
        //   2. created when we build deserializer expression for nested tuple, no `name` property.
        // Here we want to validate the ordinals of nested tuple, so we should only catch
        // `GetStructField` without the name property.
        .collect { case g: GetStructField if g.name.isEmpty => g }
        .groupBy(_.child)
        .mapValues(_.map(_.ordinal).distinct.sorted)

      structChildToOrdinals.foreach { case (expr, ordinals) =>
        val schema = expr.dataType.asInstanceOf[StructType]
        if (ordinals != schema.indices) {
          fail(schema, ordinals.last)
        }
      }
    }
  }

  /**
   * Resolves [[NewInstance]] by finding and adding the outer scope to it if the object being
   * constructed is an inner class.
   */
  object ResolveNewInstance extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case n: NewInstance if n.childrenResolved && !n.resolved =>
          val outer = OuterScopes.getOuterScope(n.cls)
          if (outer == null) {
            throw new AnalysisException(
              s"Unable to generate an encoder for inner class `${n.cls.getName}` without " +
                "access to the scope that this class was defined in.\n" +
                "Try moving this class out of its parent class.")
          }
          n.copy(outerPointer = Some(outer))
      }
    }
  }

  /**
   * Replace the [[UpCast]] expression by [[Cast]], and throw exceptions if the cast may truncate.
   */
  object ResolveUpCast extends Rule[LogicalPlan] {
    private def fail(from: Expression, to: DataType, walkedTypePath: Seq[String]) = {
      val fromStr = from match {
        case l: LambdaVariable => "array element"
        case e => e.sql
      }
      throw new AnalysisException(s"Cannot up cast $fromStr from " +
        s"${from.dataType.catalogString} to ${to.catalogString}.\n" +
        "The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
        "You can either add an explicit cast to the input data or choose a higher precision " +
        "type of the field in the target object")
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case u @ UpCast(child, _, _) if !child.resolved => u

        case UpCast(child, dt: AtomicType, _)
            if SQLConf.get.getConf(SQLConf.LEGACY_LOOSE_UPCAST) &&
              child.dataType == StringType =>
          Cast(child, dt.asNullable)

        case UpCast(child, dataType, walkedTypePath) if !Cast.canUpCast(child.dataType, dataType) =>
          fail(child, dataType, walkedTypePath)

        case UpCast(child, dataType, _) => Cast(child, dataType.asNullable)
      }
    }
  }
}

/**
 * 从计划中删除[[子查询别名]]运算符。子查询只需要提供属性的范围信息，分析完成后可以删除。
 */
object EliminateSubqueryAliases extends Rule[LogicalPlan] {
  //这也在优化阶段的开始时调用，因此
  //正在使用TransformUp而不是ResolveOperators。
  def apply(plan: LogicalPlan): LogicalPlan = AnalysisHelper.allowInvokingTransformsInAnalyzer {
    plan transformUp {
      case SubqueryAlias(_, child) => child
    }
  }
}

/**
 * Removes [[Union]] operators from the plan if it just has one child.
 */
object EliminateUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Union(children) if children.size == 1 => children.head
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions). Notice that if an expression has other expression parameters which
 * are not in its `children`, e.g. `RuntimeReplaceable`, the transformation for Aliases in this
 * rule can't work for those parameters.
 */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    e.transformDown {
      case Alias(child, _) => child
      case MultiAlias(child, _) => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      a.copy(child = trimAliases(a.child))(
        exprId = a.exprId,
        qualifier = a.qualifier,
        explicitMetadata = Some(a.metadata))
    case a: MultiAlias =>
      a.copy(child = trimAliases(a.child))
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case Window(windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    // Operators that operate on objects should only have expressions from encoders, which should
    // never have extra aliases.
    case o: ObjectConsumer => o
    case o: ObjectProducer => o
    case a: AppendColumns => a

    case other =>
      other transformExpressionsDown {
        case Alias(child, _) => child
      }
  }
}

/**
 * Ignore event time watermark in batch query, which is only supported in Structured Streaming.
 * TODO: add this rule into analyzer rule list.
 */
object EliminateEventTimeWatermark extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case EventTimeWatermark(_, _, child) if !child.isStreaming => child
  }
}

/**
 * Maps a time column to multiple time windows using the Expand operator. Since it's non-trivial to
 * figure out how many windows a time column can map to, we over-estimate the number of windows and
 * filter out the rows where the time column is not inside the time window.
 */
object TimeWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private final val WINDOW_COL_NAME = "window"
  private final val WINDOW_START = "start"
  private final val WINDOW_END = "end"

  /**
   * Generates the logical plan for generating window ranges on a timestamp column. Without
   * knowing what the timestamp value is, it's non-trivial to figure out deterministically how many
   * window ranges a timestamp will map to given all possible combinations of a window duration,
   * slide duration and start time (offset). Therefore, we express and over-estimate the number of
   * windows there may be, and filter the valid windows. We use last Project operator to group
   * the window columns into a struct so they can be accessed as `window.start` and `window.end`.
   *
   * The windows are calculated as below:
   * maxNumOverlapping <- ceil(windowDuration / slideDuration)
   * for (i <- 0 until maxNumOverlapping)
   *   windowId <- ceil((timestamp - startTime) / slideDuration)
   *   windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
   *   windowEnd <- windowStart + windowDuration
   *   return windowStart, windowEnd
   *
   * This behaves as follows for the given parameters for the time: 12:05. The valid windows are
   * marked with a +, and invalid ones are marked with a x. The invalid ones are filtered using the
   * Filter operator.
   * window: 12m, slide: 5m, start: 0m :: window: 12m, slide: 5m, start: 2m
   *     11:55 - 12:07 +                      11:52 - 12:04 x
   *     12:00 - 12:12 +                      11:57 - 12:09 +
   *     12:05 - 12:17 +                      12:02 - 12:14 +
   *
   * @param plan The logical plan
   * @return the logical plan that will generate the time windows using the Expand operator, with
   *         the Filter operator for correctness and Project for usability.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowExpressions =
        p.expressions.flatMap(_.collect { case t: TimeWindow => t }).toSet

      val numWindowExpr = windowExpressions.size
      // Only support a single window expression for now
      if (numWindowExpr == 1 &&
          windowExpressions.head.timeColumn.resolved &&
          windowExpressions.head.checkInputDataTypes().isSuccess) {

        val window = windowExpressions.head

        val metadata = window.timeColumn match {
          case a: Attribute => a.metadata
          case _ => Metadata.empty
        }

        def getWindow(i: Int, overlappingWindows: Int): Expression = {
          val division = (PreciseTimestampConversion(
            window.timeColumn, TimestampType, LongType) - window.startTime) / window.slideDuration
          val ceil = Ceil(division)
          // if the division is equal to the ceiling, our record is the start of a window
          val windowId = CaseWhen(Seq((ceil === division, ceil + 1)), Some(ceil))
          val windowStart = (windowId + i - overlappingWindows) *
            window.slideDuration + window.startTime
          val windowEnd = windowStart + window.windowDuration

          CreateNamedStruct(
            Literal(WINDOW_START) ::
              PreciseTimestampConversion(windowStart, LongType, TimestampType) ::
              Literal(WINDOW_END) ::
              PreciseTimestampConversion(windowEnd, LongType, TimestampType) ::
              Nil)
        }

        val windowAttr = AttributeReference(
          WINDOW_COL_NAME, window.dataType, metadata = metadata)()

        if (window.windowDuration == window.slideDuration) {
          val windowStruct = Alias(getWindow(0, 1), WINDOW_COL_NAME)(
            exprId = windowAttr.exprId, explicitMetadata = Some(metadata))

          val replacedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          // For backwards compatibility we add a filter to filter out nulls
          val filterExpr = IsNotNull(window.timeColumn)

          replacedPlan.withNewChildren(
            Filter(filterExpr,
              Project(windowStruct +: child.output, child)) :: Nil)
        } else {
          val overlappingWindows =
            math.ceil(window.windowDuration * 1.0 / window.slideDuration).toInt
          val windows =
            Seq.tabulate(overlappingWindows)(i => getWindow(i, overlappingWindows))

          val projections = windows.map(_ +: child.output)

          val filterExpr =
            window.timeColumn >= windowAttr.getField(WINDOW_START) &&
              window.timeColumn < windowAttr.getField(WINDOW_END)

          val substitutedPlan = Filter(filterExpr,
            Expand(projections, windowAttr +: child.output, child))

          val renamedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          renamedPlan.withNewChildren(substitutedPlan :: Nil)
        }
      } else if (numWindowExpr > 1) {
        p.failAnalysis("Multiple time window expressions would result in a cartesian product " +
          "of rows, therefore they are currently not supported.")
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}

/**
 * Resolve a [[CreateNamedStruct]] if it contains [[NamePlaceholder]]s.
 */
object ResolveCreateNamedStruct extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressions {
    case e: CreateNamedStruct if !e.resolved =>
      val children = e.children.grouped(2).flatMap {
        case Seq(NamePlaceholder, e: NamedExpression) if e.resolved =>
          Seq(Literal(e.name), e)
        case kv =>
          kv
      }
      CreateNamedStruct(children.toList)
  }
}

/**
 * The aggregate expressions from subquery referencing outer query block are pushed
 * down to the outer query block for evaluation. This rule below updates such outer references
 * as AttributeReference referring attributes from the parent/outer query block.
 *
 * For example (SQL):
 * {{{
 *   SELECT l.a FROM l GROUP BY 1 HAVING EXISTS (SELECT 1 FROM r WHERE r.d < min(l.b))
 * }}}
 * Plan before the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < min(outer(b#227)))       <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
 * Plan after the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < outer(min(b#227)#249))   <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
 */
object UpdateOuterReferences extends Rule[LogicalPlan] {
  private def stripAlias(expr: Expression): Expression = expr match { case a: Alias => a.child }

  private def updateOuterReferenceInSubquery(
      plan: LogicalPlan,
      refExprs: Seq[Expression]): LogicalPlan = {
    plan resolveExpressions { case e =>
      val outerAlias =
        refExprs.find(stripAlias(_).semanticEquals(stripOuterReference(e)))
      outerAlias match {
        case Some(a: Alias) => OuterReference(a.toAttribute)
        case _ => e
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case f @ Filter(_, a: Aggregate) if f.resolved =>
        f transformExpressions {
          case s: SubqueryExpression if s.children.nonEmpty =>
            // Collect the aliases from output of aggregate.
            val outerAliases = a.aggregateExpressions collect { case a: Alias => a }
            // Update the subquery plan to record the OuterReference to point to outer query plan.
            s.withNewPlan(updateOuterReferenceInSubquery(s.plan, outerAliases))
      }
    }
  }
}
