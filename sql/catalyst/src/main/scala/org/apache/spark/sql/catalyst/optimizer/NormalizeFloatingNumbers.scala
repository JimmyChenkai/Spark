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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, ArrayTransform, CreateArray, CreateMap, CreateNamedStruct, CreateNamedStructUnsafe, CreateStruct, EqualTo, ExpectsInputTypes, Expression, GetStructField, LambdaFunction, NamedLambdaVariable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * We need to take care of special floating numbers (NaN and -0.0) in several places:
 *   1. When compare values, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   2. In aggregate grouping keys, different NaNs should belong to the same group, -0.0 and 0.0
 *      should belong to the same group.
 *   3. In join keys, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   4. In window partition keys, different NaNs should belong to the same partition, -0.0 and 0.0
 *      should belong to the same partition.
 *
 * Case 1 is fine, as we handle NaN and -0.0 well during comparison. For complex types, we
 * recursively compare the fields/elements, so it's also fine.
 *
 * Case 2, 3 and 4 are problematic, as Spark SQL turns grouping/join/window partition keys into
 * binary `UnsafeRow` and compare the binary data directly. Different NaNs have different binary
 * representation, and the same thing happens for -0.0 and 0.0.
 *
 *此规则规范化窗口分区键，连接键和聚合分组中的NaN和-0.0
 *钥匙。
 *
 *理想情况下，我们应该在比较的物理运算符中进行归一化
 *二进制`UnsafeRow`直接。如果是Spark SQL执行引擎，我们不需要这种规范化
 *未针对二进制数据运行进行优化。创建此规则是为了简化实现，因此
 *我们有一个地方可以进行标准化，这更易于维护。
 *
 *请注意，此规则必须在优化程序结束时执行，因为优化程序可能会创建
 *新连接（子查询重写）和新连接条件（连接重新排序）。
 */
object NormalizeFloatingNumbers extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    //子查询稍后将被重写为join，并将通过此规则
    //最终 这里我们跳过子查询，因为我们只需要运行一次这个规则。
    case _: Subquery => plan

    case _ => plan transform {
      case w: Window if w.partitionSpec.exists(p => needNormalize(p.dataType)) =>
        //虽然`windowExpressions`可能引用`partitionSpec`表达式，但我们不需要
        //规范化`windowExpressions`，因为它们是按输入行执行的，应该采用
        //输入行原样。
        w.copy(partitionSpec = w.partitionSpec.map(normalize))

      //只有散列连接和排序合并连接需要规范化。在这里，我们捕获所有联接
      //连接键，假设连接键的连接总是计划为散列连接或排序合并
      //加入 我们不太可能在不久的将来打破这一假设。
      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _)
          //分析器保证左右连接键具有相同的数据类型。在这里，我们
          //只需要检查一方的连接键。
          if leftKeys.exists(k => needNormalize(k.dataType)) =>
        val newLeftJoinKeys = leftKeys.map(normalize)
        val newRightJoinKeys = rightKeys.map(normalize)
        val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
          case (l, r) => EqualTo(l, r)
        } ++ condition
        j.copy(condition = Some(newConditions.reduce(And)))

      // TODO：理想情况下，聚合也应该在这里处理，但它的分组表达式是
      //在其聚合表达式中混合。更改分组表达式是不可靠的
      //这里 现在我们在规划期间规范化`AggUtils`中的分组表达式。
    }
  }

  private def needNormalize(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case StructType(fields) => fields.exists(f => needNormalize(f.dataType))
    case ArrayType(et, _) => needNormalize(et)
    //当前MapType无法比较，如果发生这种情况，分析器应该提前失败。
    case _: MapType =>
      throw new IllegalStateException("grouping/join/window partition keys cannot be map type.")
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr.dataType) => expr

    case a: Alias =>
      a.withNewChildren(Seq(normalize(a.child)))

    case CreateNamedStruct(children) =>
      CreateNamedStruct(children.map(normalize))

    case CreateNamedStructUnsafe(children) =>
      CreateNamedStructUnsafe(children.map(normalize))

    case CreateArray(children) =>
      CreateArray(children.map(normalize))

    case CreateMap(children) =>
      CreateMap(children.map(normalize))

    case _ if expr.dataType == FloatType || expr.dataType == DoubleType =>
      NormalizeNaNAndZero(expr)

    case _ if expr.dataType.isInstanceOf[StructType] =>
      val fields = expr.dataType.asInstanceOf[StructType].fields.indices.map { i =>
        normalize(GetStructField(expr, i))
      }
      CreateStruct(fields)

    case _ if expr.dataType.isInstanceOf[ArrayType] =>
      val ArrayType(et, containsNull) = expr.dataType
      val lv = NamedLambdaVariable("arg", et, containsNull)
      val function = normalize(lv)
      ArrayTransform(expr, LambdaFunction(function, Seq(lv)))

    case _ => throw new IllegalStateException(s"fail to normalize $expr")
  }
}

case class NormalizeNaNAndZero(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(FloatType, DoubleType))

  private lazy val normalizer: Any => Any = child.dataType match {
    case FloatType => (input: Any) => {
      val f = input.asInstanceOf[Float]
      if (f.isNaN) {
        Float.NaN
      } else if (f == -0.0f) {
        0.0f
      } else {
        f
      }
    }

    case DoubleType => (input: Any) => {
      val d = input.asInstanceOf[Double]
      if (d.isNaN) {
        Double.NaN
      } else if (d == -0.0d) {
        0.0d
      } else {
        d
      }
    }
  }

  override def nullSafeEval(input: Any): Any = {
    normalizer(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val codeToNormalize = child.dataType match {
      case FloatType => (f: String) => {
        s"""
           |if (Float.isNaN($f)) {
           |  ${ev.value} = Float.NaN;
           |} else if ($f == -0.0f) {
           |  ${ev.value} = 0.0f;
           |} else {
           |  ${ev.value} = $f;
           |}
         """.stripMargin
      }

      case DoubleType => (d: String) => {
        s"""
           |if (Double.isNaN($d)) {
           |  ${ev.value} = Double.NaN;
           |} else if ($d == -0.0d) {
           |  ${ev.value} = 0.0d;
           |} else {
           |  ${ev.value} = $d;
           |}
         """.stripMargin
      }
    }

    nullSafeCodeGen(ctx, ev, codeToNormalize)
  }
}
