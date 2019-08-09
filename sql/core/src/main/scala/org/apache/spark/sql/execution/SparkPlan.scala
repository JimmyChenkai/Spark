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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.{broadcast, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

object SparkPlan {
  /** The original [[LogicalPlan]] from which this [[SparkPlan]] is converted. */
  val LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("logical_plan")

  /** The [[LogicalPlan]] inherited from its ancestor. */
  val LOGICAL_PLAN_INHERITED_TAG = TreeNodeTag[LogicalPlan]("logical_plan_inherited")
}

/**
 * physical operators的基类
 *
 * 命名约定是物理运算符以“Exec”后缀结尾，例如[[ ProjectExec ]]。
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * sqlContext用来创建此计划上下文预置环境变量的。
   * 很多操作是需要访问sqlContext来进行RDD操作的
   */
  @transient final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  protected def sparkContext = sqlContext.sparkContext

  // /在没有活动会话的情况下创建SparkPlan节点时，sqlContext将为null。
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  // 在遇到由codegen引起的编译错误时是否应该回退
  private val codeGenFallBack = (sqlContext == null) || sqlContext.conf.codegenFallback

  // 重写的make copy也会将sqlContext传播到复制的计划。
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    if (sqlContext != null) {
      SparkSession.setActiveSession(sqlContext.sparkSession)
    }
    super.makeCopy(newArgs)
  }

  /**
   * 返回此计划所链接的逻辑计划。
   */
  def logicalLink: Option[LogicalPlan] =
    getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      .orElse(getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))

  /**
   * 如果未设置，则递归设置逻辑计划链接。
   */
  def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    setLogicalLink(logicalPlan, false)
  }

  private def setLogicalLink(logicalPlan: LogicalPlan, inherited: Boolean = false): Unit = {
    // Stop at a descendant which is the root of a sub-tree transformed from another logical node.
    if (inherited && getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isDefined) {
      return
    }

    val tag = if (inherited) {
      SparkPlan.LOGICAL_PLAN_INHERITED_TAG
    } else {
      SparkPlan.LOGICAL_PLAN_TAG
    }
    setTagValue(tag, logicalPlan)
    children.foreach(_.setLogicalLink(logicalPlan, true))
  }

  /**
   * 返回包含此SparkPlan指标的所有指标。
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * 重置所有指标。
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
  }

  /**
   * 返回[[ SQLMetric ]]代表`name`。
   */
  def longMetric(name: String): SQLMetric = metrics(name)

   // TODO：转到`DistributedPlan`
   /**指定如何跨群集中的不同节点对数据进行分区。*/
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /**
   * Specifies the data distribution requirements of all the children for this operator. By default
   * it's [[UnspecifiedDistribution]] for each child, which means each child can have any
   * distribution.
   *
   * If an operator overwrites this method, and specifies distribution requirements(excluding
   * [[UnspecifiedDistribution]] and [[BroadcastDistribution]]) for more than one child, Spark
   * guarantees that the outputs of these children will have same number of partitions, so that the
   * operator can safely zip partitions of these children's result RDDs. Some operators can leverage
   * this guarantee to satisfy some interesting requirement, e.g., non-broadcast joins can specify
   * HashClusteredDistribution(a,b) for its left child, and specify HashClusteredDistribution(c,d)
   * for its right child, then it's guaranteed that left and right child are co-partitioned by
   * a,b/c,d, which means tuples of same value are in the partitions of same index, e.g.,
   * (a=1,b=2) and (c=1,d=2) are both in the second partition of left and right child.
   */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /**指定每个分区中数据的排序方式。*/
  def outputOrdering: Seq[SortOrder] = Nil

  /**指定此运算符的输入数据上每个分区要求的排序顺序*/
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * 通过委托给`doExecute`后，将此查询的结果作为RDD [InternalRow]返回
   *
   * SparkPlan的具体实现应该覆盖`doExecute`。
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }

  /**
   * 准备好之后通过委托给`doExecuteBroadcast`将此查询的结果作为广播变量返回
   *
   * SparkPlan的具体实现应该覆盖`doExecuteBroadcast`。
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteBroadcast()
  }

  /**
   * 在准备查询并向创建的RDD添加查询计划信息后执行查询
   * 用于可视化
   */
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query
    }
  }

  /**
   * 此计划节点的（不相关的标量子查询，将来保存子查询结果）的列表。
   * 此列表由[[ prepareSubqueries ]] 填充，在[[ prepare ]]中调用。
   */
  @transient
  private val runningSubqueries = new ArrayBuffer[ExecSubqueryExpression]

  /**
   * 在此计划节点中查找标量子查询表达式并开始评估它们。
   */
  protected def prepareSubqueries(): Unit = {
    expressions.foreach {
      _.collect {
        case e: ExecSubqueryExpression =>
          e.plan.prepare()
          runningSubqueries += e
      }
    }
  }

  /**
   * 阻塞线程，直到所有子查询完成评估并更新结果。
   */
  protected def waitForSubqueries(): Unit = synchronized {
    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }
    runningSubqueries.clear()
  }

  /**
   * 是否调用“prepare”方法。
   */
  private var prepared = false

  /**
   *准备此SparkPlan以执行。它是幂等的。
   */
  final def prepare(): Unit = {
    // doPrepare（）可能依赖于它的孩子，我们应该首先对所有孩子调用prepare（）。
    children.foreach(_.prepare())
    synchronized {
      if (!prepared) {
        prepareSubqueries()
        doPrepare()
        prepared = true
      }
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * @note `prepare` method has already walked down the tree, so the implementation doesn't have
   * to call children's `prepare` methods.
   *
   * This will only be called once, protected by `this`.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Produces the result of the query as a broadcast variable.
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw new UnsupportedOperationException(s"$nodeName does not implement doExecuteBroadcast")
  }

  /**
   *将UnsafeRows打包成字节数组，以便更快地进行序列化。
   *字节数组采用以下格式：
   * [size] [UnsafeRow的字节] [size] [UnsafeRow的字节] ... [-1]
   *
   * UnsafeRow是高度可压缩的（任何列至少8个字节），字节数组也是
   * 压缩。
   */
  private def getByteArrayRdd(n: Int = -1): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      // `iter.hasNext`可以生成一行并缓冲它，我们只应在限制时调用它
      // not hit.
      while ((n < 0 || count < n) && iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }

  /**
   * 将字节数组解码回UnsafeRows并将它们放入缓冲区。
   */
  private def decodeUnsafeRows(bytes: Array[Byte]): Iterator[InternalRow] = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))

    new Iterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      override def hasNext: Boolean = sizeOfNextRow >= 0
      override def next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }
    }
  }

  /**
   * 运行此查询将结果作为数组返回。
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }

  private[spark] def executeCollectIterator(): (Long, Iterator[InternalRow]) = {
    val countsAndBytes = getByteArrayRdd().collect()
    val total = countsAndBytes.map(_._1).sum
    val rows = countsAndBytes.iterator.flatMap(countAndBytes => decodeUnsafeRows(countAndBytes._2))
    (total, rows)
  }

  /**
   * Runs this query returning the result as an iterator of InternalRow.
   *
   * @note Triggers multiple jobs (one for each partition).
   */
  def executeToIterator(): Iterator[InternalRow] = {
    getByteArrayRdd().map(_._2).toLocalIterator.flatMap(decodeUnsafeRows)
  }

  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * 运行此查询，将第一行“n”行作为数组返回
   *
   * 这是在`RDD.take`之后建模的，但从不在驱动程序上本地运行任何作业。
   */
  def executeTake(n: Int): Array[InternalRow] = {
    //如果n==0 则新建一个InternalRow类型的Array
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = getByteArrayRdd(n).map(_._2)

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      //在此迭代中尝试的分区数。这个号码可以大于totalParts，因为我们实际上将它限制在runJob中的totalParts。
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        //如果我们在上一次迭代后没有找到任何行，则重复四次并重试。
        //否则，插入我们需要尝试的分区数，但要高估50％我们最终也将估算上限。
        val limitScaleUpFactor = Math.max(sqlContext.conf.limitScaleUpFactor, 2)
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          val left = n - buf.size
          // 例如 left > 0, numPartsToTry>=1是始终这样的
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.size).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Array[Byte]]) => if (it.hasNext) it.next() else Array.empty[Byte], p)

      buf ++= res.flatMap(decodeUnsafeRows)

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): MutableProjection = {
    log.debug(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    MutableProjection.create(expressions, inputSchema)
  }

  private def genInterpretedPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): InterpretedPredicate = {
    val str = expression.toString
    val logMessage = if (str.length > 256) {
      str.substring(0, 256 - 3) + "..."
    } else {
      str
    }
    logWarning(s"Codegen disabled for this expression:\n $logMessage")
    InterpretedPredicate.create(expression, inputSchema)
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): GenPredicate = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case _ @ (_: InternalCompilerException | _: CompileException) if codeGenFallBack =>
        genInterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    GenerateOrdering.generate(order, inputSchema)fg
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SparkPlan, SparkPlan)] = a match {
    case s: SparkPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}
