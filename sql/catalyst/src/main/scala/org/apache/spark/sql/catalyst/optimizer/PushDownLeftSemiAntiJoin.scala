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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This rule is a variant of [[PushDownPredicate]] which can handle
 * pushing down Left semi and Left Anti joins below the following operators.
 *  1) Project
 *  2) Window
 *  3) Union
 *  4) Aggregate
 *  5) Other permissible unary operators. please see [[PushDownPredicate.canPushThrough]].
 */
object PushDownLeftSemiAntiJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // LeftSemi/LeftAnti over Project
    case Join(p @ Project(pList, gChild), rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
        if pList.forall(_.deterministic) &&
        !pList.exists(ScalarSubquery.hasCorrelatedScalarSubquery) &&
        canPushThroughCondition(Seq(gChild), joinCond, rightOp) =>
      if (joinCond.isEmpty) {
        // No join condition, just push down the Join below Project
        p.copy(child = Join(gChild, rightOp, joinType, joinCond, hint))
      } else {
        val aliasMap = PushDownPredicate.getAliasMap(p)
        val newJoinCond = if (aliasMap.nonEmpty) {
          Option(replaceAlias(joinCond.get, aliasMap))
        } else {
          joinCond
        }
        p.copy(child = Join(gChild, rightOp, joinType, newJoinCond, hint))
      }

    // LeftSemi/LeftAnti over Aggregate
    case join @ Join(agg: Aggregate, rightOp, LeftSemiOrAnti(_), _, _)
        if agg.aggregateExpressions.forall(_.deterministic) && agg.groupingExpressions.nonEmpty &&
        !agg.aggregateExpressions.exists(ScalarSubquery.hasCorrelatedScalarSubquery) =>
      val aliasMap = PushDownPredicate.getAliasMap(agg)
      val canPushDownPredicate = (predicate: Expression) => {
        val replaced = replaceAlias(predicate, aliasMap)
        predicate.references.nonEmpty &&
          replaced.references.subsetOf(agg.child.outputSet ++ rightOp.outputSet)
      }
      val makeJoinCondition = (predicates: Seq[Expression]) => {
        replaceAlias(predicates.reduce(And), aliasMap)
      }
      pushDownJoin(join, canPushDownPredicate, makeJoinCondition)

    // LeftSemi/LeftAnti over Window
    case join @ Join(w: Window, rightOp, LeftSemiOrAnti(_), _, _)
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references)) ++ rightOp.outputSet
      pushDownJoin(join, _.references.subsetOf(partitionAttrs), _.reduce(And))

    // LeftSemi/LeftAnti over Union
    case Join(union: Union, rightOp, LeftSemiOrAnti(joinType), joinCond, hint)
        if canPushThroughCondition(union.children, joinCond, rightOp) =>
      if (joinCond.isEmpty) {
        // Push down the Join below Union
        val newGrandChildren = union.children.map { Join(_, rightOp, joinType, joinCond, hint) }
        union.withNewChildren(newGrandChildren)
      } else {
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = joinCond.get transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet ++ rightOp.outputSet))
          Join(grandchild, rightOp, joinType, Option(newCond), hint)
        }
        union.withNewChildren(newGrandChildren)
      }

    // LeftSemi/LeftAnti over UnaryNode
    case join @ Join(u: UnaryNode, rightOp, LeftSemiOrAnti(_), _, _)
        if PushDownPredicate.canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      val validAttrs = u.child.outputSet ++ rightOp.outputSet
      pushDownJoin(join, _.references.subsetOf(validAttrs), _.reduce(And))
  }

  /**
   * 通过确保属性，检查我们是否可以通过项目或联合安全地推送连接
   * 在连接条件中引用的属性与它们移动的计划不包含相同的属性进入。
   * 当连接的两端引用相同的源（自连接）时，可能会发生这种情况。这个
   * function确保连接条件引用不明确的属性（即
   * 出现在联接的两条腿中）或者由此产生的计划无效。
   */
  private def canPushThroughCondition(
      plans: Seq[LogicalPlan],
      condition: Option[Expression],
      rightOp: LogicalPlan): Boolean = {
    val attributes = AttributeSet(plans.flatMap(_.output))
    if (condition.isDefined) {
      val matched = condition.get.references.intersect(rightOp.outputSet).intersect(attributes)
      matched.isEmpty
    } else {
      true
    }
  }

  private def pushDownJoin(
      join: Join,
      canPushDownPredicate: Expression => Boolean,
      makeJoinCondition: Seq[Expression] => Expression): LogicalPlan = {
    assert(join.left.children.length == 1)

    if (join.condition.isEmpty) {
      join.left.withNewChildren(Seq(join.copy(left = join.left.children.head)))
    } else {
      val (pushDown, stayUp) = splitConjunctivePredicates(join.condition.get)
        .partition(canPushDownPredicate)

      //检查剩余的谓词是否不包含右侧的列
      //加入 由于剩余的谓词将作为过滤器保留在加入的运算符之上，
      //在按下左半/反连接后，必须进行此检查。原因是，为
      //这种连接，我们只从连接的左腿输出。
      val referRightSideCols = AttributeSet(stayUp.toSet).intersect(join.right.outputSet).nonEmpty

      if (pushDown.isEmpty || referRightSideCols)  {
        join
      } else {
        val newPlan = join.left.withNewChildren(Seq(join.copy(
          left = join.left.children.head, condition = Some(makeJoinCondition(pushDown)))))
        // 如果没有更多过滤器可以停留，请返回已按下加入的新计划。
        if (stayUp.isEmpty) {
          newPlan
        } else {
          join.joinType match {
            //在左半连接的情况下，连接条件的一部分没有引用
            //将孙子的属性保存为上面的过滤器。
            case LeftSemi => Filter(stayUp.reduce(And), newPlan)
             //如果是左反连接，则仅在整个连接时按下连接
            //条件有资格被下推以保留left-anti join的语义。
            case _ => join
          }
        }
      }
    }
  }
}

/**
 * This rule is a variant of [[PushPredicateThroughJoin]] which can handle
 * pushing down Left semi and Left Anti joins below a join operator. The
 * allowable join types are:
 *  1) Inner
 *  2) Cross
 *  3) LeftOuter
 *  4) RightOuter
 *
 * TODO:
 * 目前此规则可以将左半部分或左部反连接向下推
 * 孩子的左腿或右腿加入。这符合`PushPredicateThroughJoin`的行为
 * 当lefi半连接或左反连接处于表达形式时。我们需要探索这种可能性
 * 如果连接条件指的话，将左半连接/反连接推到连接的两条腿
 * 孩子的左腿和右腿都加入了。
 */
object PushLeftSemiLeftAntiThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * 定义一个枚举，以确定是否可以将LeftSemi / LeftAnti连接推送到
   * 连接的左腿或右腿。
   */
  object PushdownDirection extends Enumeration {
    val TO_LEFT_BRANCH, TO_RIGHT_BRANCH, NONE = Value
  }

  object AllowedJoin {
    def unapply(join: Join): Option[Join] = join.joinType match {
      case Inner | Cross | LeftOuter | RightOuter => Some(join)
      case _ => None
    }
  }

  /**
   * 确定可以将LeftSemi / LeftAnti连接的哪一侧连接到。
   */
  private def pushTo(leftChild: Join, rightChild: LogicalPlan, joinCond: Option[Expression]) = {
    val left = leftChild.left
    val right = leftChild.right
    val joinType = leftChild.joinType
    val rightOutput = rightChild.outputSet

    if (joinCond.nonEmpty) {
      val conditions = splitConjunctivePredicates(joinCond.get)
      val (leftConditions, rest) =
        conditions.partition(_.references.subsetOf(left.outputSet ++ rightOutput))
      val (rightConditions, commonConditions) =
        rest.partition(_.references.subsetOf(right.outputSet ++ rightOutput))

      if (rest.isEmpty && leftConditions.nonEmpty) {
        // 当连接条件可以根据左腿计算时
        // leftsemi / anti join然后将leftsemi / anti join推到左侧。
        PushdownDirection.TO_LEFT_BRANCH
      } else if (leftConditions.isEmpty && rightConditions.nonEmpty && commonConditions.isEmpty) {
        // 当可以根据右腿的属性计算连接条件时
        // leftsemi / anti join然后将leftsemi / anti join推到右侧。
        PushdownDirection.TO_RIGHT_BRANCH
      } else {
        PushdownDirection.NONE
      }
    } else {
      /**
       *当连接条件为空时，
       * 1）如果这是左外连接或内连接，则按下leftsemi / anti join
       * 加入左。
       * 2）如果右外连接，连接，
       */
      joinType match {
        case _: InnerLike | LeftOuter =>
          PushdownDirection.TO_LEFT_BRANCH
        case RightOuter =>
          PushdownDirection.TO_RIGHT_BRANCH
        case _ =>
          PushdownDirection.NONE
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //将LeftSemi / LeftAnti向下推入下面的连接
    case j @ Join(AllowedJoin(left), right, LeftSemiOrAnti(joinType), joinCond, parentHint) =>
      val (childJoinType, childLeft, childRight, childCondition, childHint) =
        (left.joinType, left.left, left.right, left.condition, left.hint)
      val action = pushTo(left, right, joinCond)

      action match {
        case PushdownDirection.TO_LEFT_BRANCH
          if (childJoinType == LeftOuter || childJoinType.isInstanceOf[InnerLike]) =>
          //将leftsemi / anti join下拉到左表
          val newLeft = Join(childLeft, right, joinType, joinCond, parentHint)
          Join(newLeft, childRight, childJoinType, childCondition, childHint)
        case PushdownDirection.TO_RIGHT_BRANCH
          if (childJoinType == RightOuter || childJoinType.isInstanceOf[InnerLike]) =>
          //将leftsemi / anti join下推到右边的表格
          val newRight = Join(childRight, right, joinType, joinCond, parentHint)
          Join(childLeft, newRight, childJoinType, childCondition, childHint)
        case _ =>
          //什么都不做
          j
      }
  }
}


