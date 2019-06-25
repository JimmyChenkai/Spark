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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits or abstract classes:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.
 * - [[Stateful]]: an expression that contains mutable state. For example, MonotonicallyIncreasingID
 *                 and Rand. A stateful expression is always non-deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 * - [[NullIntolerant]]: an expression that is null intolerant (i.e. any null input will result in
 *                       null output).
 * - [[NonSQLExpression]]: a common base trait for the expressions that do not have SQL
 *                         expressions like representation. For example, `ScalaUDF`, `ScalaUDAF`,
 *                         and object `MapObjects` and `Invoke`.
 * - [[UserDefinedExpression]]: a common base trait for user-defined functions, including
 *                              UDF/UDAF/UDTF.
 * - [[HigherOrderFunction]]: a common base trait for higher order functions that take one or more
 *                            (lambda) functions and applies these to some objects. The function
 *                            produces a number of variables which can be consumed by some lambda
 *                            functions.
 * - [[NamedExpression]]: An [[Expression]] that is named.
 * - [[TimeZoneAwareExpression]]: A common base trait for time zone aware expressions.
 * - [[SubqueryExpression]]: A base interface for expressions that contain a [[LogicalPlan]].
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[TernaryExpression]]: an expression that has three children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 * A few important traits used for type coercion rules:
 * - [[ExpectsInputTypes]]: an expression that has the expected input types. This trait is typically
 *                          used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 *                          expected input types without any implicit casting.
 * - [[ImplicitCastInputTypes]]: an expression that has the expected input types, which can be
 *                               implicitly castable using [[TypeCoercion.ImplicitTypeCasts]].
 * - [[ComplexTypeMergingExpression]]: to resolve output types of the complex expressions
 *                                     (e.g., [[CaseWhen]]).
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   *  该属性用来标记表达式能否在查询执行之前直接静态计算。 
   *  foldable 为true 的情况有两种，
   *  一：是该表达式为 Literal 类型（“字面值”，例如常量等）
   *  二：当且仅当其子表达式中foldable都为true时 如下
   *     - [[Coalesce]] 当所有孩子节点foldable均为true
   *     - [[BinaryExpression]] 左右孩子节点foldable均为true
   *     - [[Not]], [[IsNull]], [[IsNotNull]] 孩子节点foldable均为true
   *     - [[Literal]] foldabel为true
   *     - [[Cast]] 或 [[UnaryMinus]] 孩子节点foldable均为true
   */
  def foldable: Boolean = false

  /**
   * 当前表达式始终为来自的固定输入返回相同结果时，返回true。
   * 子节点。非确定性表达式不应在数量和顺序上发生变化。他们应该
   * 在查询计划期间不进行评估。
   *
   * 注意，这意味着一个表达式应该被认为是不确定性的。如果:
   * - 它依赖于一些可变的内部状态，或者
   * - 它依赖一些不属于子表达式列表的隐式输入。
   * - 它有一个或多个不确定的孩子节点。
   * - 它假定输入通过子运算符满足某些特定条件。
   *
   * “sparkPartitionID”就是一个例子，它依赖于taskContext返回的分区ID。
   * 默认情况下，叶表达式是确定性的，为零。forall（.determinatic）返回true。
   */
  lazy val deterministic: Boolean = children.forall(_.deterministic)

  //该属性用来标记表达式是否可能输出 Null 值， 一般在生成的 J肝a 代码中对相关条件进行判断 
  def nullable: Boolean

  /**
   * Workaround scala compiler so that we can call super on lazy vals
   */
  @transient
  private lazy val _references: AttributeSet =
    AttributeSet.fromAttributeSets(children.map(_.references))

  //返回值为 AttributeSet 类型，表示该 Expression 中会涉及的属性值，默认情况为所有子节点中属性值的集合。
  def references: AttributeSet = _references

  /** 一条输入，并返回该输入对应的表示结果 */
  def eval(input: InternalRow = null): Any

  /**
   * 返回一个多维数组[[ExprCode]] ，
   * 这个多维数组包含java源码，该java源码是依据输入的row而产生的对应的表达式
   *
   * @param ctx a [[CodegenContext]]
   * @return [[ExprCode]]
   */
  def genCode(ctx: CodegenContext): ExprCode = {
    ctx.subExprEliminationExprs.get(this).map { subExprState =>
      // 此表达式重复，这意味着用于计算它的代码以前已作为函数添加。在这种情况下，我们只是重复使用它。
      ExprCode(ctx.registerComment(this.toString), subExprState.isNull, subExprState.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val value = ctx.freshName("value")
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)
      if (eval.code.toString.nonEmpty) {
        // 备注中添加 'this'
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }

  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    val splitThreshold = SQLConf.get.methodSplitThreshold
    if (eval.code.length > splitThreshold && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * 返回可以编译此表达式的Java源代码。
   * 默认是调用表达式的eval入口方法。
   * 具体的表达式实现应该重写它来执行实际的代码生成。
   *
   * @param ctx a [[CodegenContext]]
   * @param ev an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * 如果已将此表达式及其所有子级解析为特定的架构并通过了输入数据类型检查，则返回“true”；
   * 如果仍包含任何未解析的占位符或数据类型不匹配，则返回“false”。
   * 如果此类型的解析为表达式不仅仅涉及其子级的解析和类型检查。
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * 返回计算此表达式的结果的[[DataType]]。它是
   * 查询未解析表达式的数据类型无效（即当'resolved`==false'时）。
   */
  def dataType: DataType

  /**
   * 如果此表达式的所有子级都已解析为特定架构，则返回true
   * 如果仍然包含任何未解析的占位符，则返回false。
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * 返回经过规范化（ Canonicalize ）处理后的表达式。 规范化处理会在确保输
   * 出结果相同的前提下通过一些规则对表达式进行重写，具体逻辑可以参见 Canonicalize 工具类
   *
   * 返回一个表达式，其中已尽最大努力以某种方式转换“this”
   * 它保留了结果，但消除了外观变化（区分大小写，订购
   * 交换操作等）有关详细信息，请参阅[[规范化]]。
   *
   * ` deterministic`expressions where`this.canonicalized==other.canonicalized`将始终评估结果相同。
   */
  lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    Canonicalize.execute(withNewChildren(canonicalizedChildren))
  }

  /**
   * 当两个表达式总是计算相同的结果时返回true，即使它们不同
   * 修饰（即属性中名称的大写可能不同）。
   *
   * 更多细节[[Canonicalize]] 
   */
  def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /**
   * 返回此表达式执行的计算的“hashcode”。不同于标准
   * ` hashcode`，试图消除外观差异。
   *
   * 更多细节[[Canonicalize]] 
   */
  def semanticHash(): Int = canonicalized.hashCode()

  /**
   * 检查输入数据类型，返回'typecheckresult.success'如果有效，
   * 或者返回“typecheckresult”，如果无效，则返回错误消息。
   * 注意：在'childrenresolved==true'之前调用此方法是无效的。
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * 返回此表达式名称的面向用户的字符串表示形式。
   * 这通常应该与SQL中的函数名匹配。
   */
  def prettyName: String = nodeName.toLowerCase(Locale.ROOT)

  //参数扁平化，这个比较好理解
  protected def flatArguments: Iterator[Any] = productIterator.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // 将此标记为final，不应调用expression.verbosestring，因此不应由具体类重写。
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  /**
   * 返回此表达式的SQL表示形式。对于扩展的表达式[[nonsqlexpression]],
   * 此方法可能返回任意面向用户的字符串。
   */
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }
}


/**
 * An expression that cannot be evaluated. These expressions don't live past analysis or
 * optimization time (e.g. Star) and should not be evaluated during query planning and
 * execution.
 */
trait Unevaluable extends Expression {

  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")
}


/**
 * An expression that gets replaced at runtime (currently by the optimizer) into a different
 * expression for evaluation. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 *
 * A RuntimeReplaceable should have the original parameters along with a "child" expression in the
 * case class constructor, and define a normal constructor that accepts only the original
 * parameters. For an example, see [[Nvl]]. To make sure the explain plan and expression SQL
 * works correctly, the implementation should also override flatArguments method and sql method.
 */
trait RuntimeReplaceable extends UnaryExpression with Unevaluable {
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = child.foldable
  override def dataType: DataType = child.dataType
  // As this expression gets replaced at optimization with its `child" expression,
  // two `RuntimeReplaceable` are considered to be semantically equal if their "child" expressions
  // are semantically equal.
  override lazy val canonicalized: Expression = child.canonicalized
}

/**
 * An aggregate expression that gets rewritten (currently by the optimizer) into a
 * different aggregate expression for evaluation. This is mainly used to provide compatibility
 * with other databases. For example, we use this to support every, any/some aggregates by rewriting
 * them with Min and Max respectively.
 */
trait UnevaluableAggregate extends DeclarativeAggregate {

  override def nullable: Boolean = true

  override lazy val aggBufferAttributes =
    throw new UnsupportedOperationException(s"Cannot evaluate aggBufferAttributes: $this")

  override lazy val initialValues: Seq[Expression] =
    throw new UnsupportedOperationException(s"Cannot evaluate initialValues: $this")

  override lazy val updateExpressions: Seq[Expression] =
    throw new UnsupportedOperationException(s"Cannot evaluate updateExpressions: $this")

  override lazy val mergeExpressions: Seq[Expression] =
    throw new UnsupportedOperationException(s"Cannot evaluate mergeExpressions: $this")

  override lazy val evaluateExpression: Expression =
    throw new UnsupportedOperationException(s"Cannot evaluate evaluateExpression: $this")
}

/**
 * Expressions that don't have SQL representation should extend this trait.  Examples are
 * `ScalaUDF`, `ScalaUDAF`, and object expressions like `MapObjects` and `Invoke`.
 */
trait NonSQLExpression extends Expression {
  final override def sql: String = {
    transform {
      case a: Attribute => new PrettyAttribute(a)
      case a: Alias => PrettyAttribute(a.sql, a.dataType)
    }.toString
  }
}


/**
 * An expression that is nondeterministic.
 */
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false

  @transient
  private[this] var initialized = false

  /**
   * Initializes internal states given the current partition index and mark this as initialized.
   * Subclasses should override [[initializeInternal()]].
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * Throws an exception if [[initialize()]] is not called yet.
   * Subclasses should override [[evalInternal()]].
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized,
      s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}

/**
 * An expression that contains mutable state. A stateful expression is always non-deterministic
 * because the results it produces during evaluation are not only dependent on the given input
 * but also on its internal state.
 *
 * The state of the expressions is generally not exposed in the parameter list and this makes
 * comparing stateful expressions problematic because similar stateful expressions (with the same
 * parameter list) but with different internal state will be considered equal. This is especially
 * problematic during tree transformations. In order to counter this the `fastEquals` method for
 * stateful expressions only returns `true` for the same reference.
 *
 * A stateful expression should never be evaluated multiple times for a single row. This should
 * only be a problem for interpreted execution. This can be prevented by creating fresh copies
 * of the stateful expression before execution, these can be made using the `freshCopy` function.
 */
trait Stateful extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): Stateful

  /**
   * Only the same reference is considered equal.
   */
  override def fastEquals(other: TreeNode[_]): Boolean = this eq other
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  override final def children: Seq[Expression] = Nil
}


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression {

  def child: Expression

  override final def children: Seq[Expression] = child :: Nil

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override final def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (!left.dataType.sameType(right.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${left.dataType.catalogString} and ${right.dataType.catalogString}).")
    } else if (!inputType.acceptsType(left.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"'$sql' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.catalogString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"
}


object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs(0).eval(input)
    if (value1 != null) {
      val value2 = exprs(1).eval(input)
      if (value2 != null) {
        val value3 = exprs(2).eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    sys.error(s"TernaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts three variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 3 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    val leftGen = children(0).genCode(ctx)
    val midGen = children(1).genCode(ctx)
    val rightGen = children(2).genCode(ctx)
    val resultCode = f(leftGen.value, midGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(children(0).nullable, leftGen.isNull) {
          midGen.code + ctx.nullSafeExec(children(1).nullable, midGen.isNull) {
            rightGen.code + ctx.nullSafeExec(children(2).nullable, rightGen.isNull) {
              s"""
                ${ev.isNull} = false; // resultCode could change nullability.
                $resultCode
              """
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${midGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * A trait used for resolving nullable flags, including `nullable`, `containsNull` of [[ArrayType]]
 * and `valueContainsNull` of [[MapType]], containsNull, valueContainsNull flags of the output date
 * type. This is usually utilized by the expressions (e.g. [[CaseWhen]]) that combine data from
 * multiple child expressions of non-primitive types.
 */
trait ComplexTypeMergingExpression extends Expression {

  /**
   * A collection of data types used for resolution the output type of the expression. By default,
   * data types of all child expressions. The collection must not be empty.
   */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(
      inputTypesForMerging.nonEmpty,
      "The collection of input data types must not be empty.")
    require(
      TypeCoercion.haveSameType(inputTypesForMerging),
      "All input types must be the same except nullable, containsNull, valueContainsNull flags." +
        s" The input types found are\n\t${inputTypesForMerging.mkString("\n\t")}")
  }

  override def dataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }
}

/**
 * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
 * and Hive function wrappers.
 */
trait UserDefinedExpression
