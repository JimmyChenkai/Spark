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

import java.util.concurrent.TimeUnit._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{EmptyRDD, PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}

/** Project的物理执行计划. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  //输出属性集合  
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  //输入RDD InternalRow
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }
  
  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def usedInputs: AttributeSet = {
    //在此计划之前，应仅评估至少使用两次的属性，
    //否则我们可以推迟评估，直到实际使用输出属性。
    val usedExprIds = projectList.flatMap(_.collect {
      case a: Attribute => a.exprId
    })
    val usedMoreThanOnce = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(a => usedMoreThanOnce.contains(a.exprId))
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val exprs = bindReferences[Expression](projectList, child.output)
    val resultVars = exprs.map(_.genCode(ctx))
    //不能推迟评估非确定性表达式。
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val project = UnsafeProjection.create(projectList, child.output)
      project.initialize(index)
      iter.map(project)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}


/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = FalseLiteral
      }
      ev
    }

    // Note: wrap in "do { } while(false);", so the generated checks can jump out with "continue;"
    s"""
       |do {
       |  $generated
       |  $nullChecks
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars)}
       |} while(false);
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val predicate = newPredicate(condition, child.output)
      predicate.initialize(0)
      iter.filter { row =>
        val r = predicate.eval(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * 采样数据集的物理计划。
 *
 * @param  lowerBound采样概率的下限（通常为0.0）
 * @param  upperBound采样概率的上限。采样的预期分数
 *         将是ub  -  lb.
 * @param  withReplacement是否更换样品。
 * @param  播种随机种子
 * @param  孩子 SparkPlan
 */
case class SampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (withReplacement) {
      //禁用间隙采样，因为间隙采样方法在内部缓冲两行，
      //要求我们复制行，这比随机数生成器贵。
      new PartitionwiseSampledRDD[InternalRow, InternalRow](
        child.execute(),
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false),
        preservesPartitioning = true,
        seed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, seed)
    }
  }

  //将此标记为空 该计划不需要评估任何输入，可以推迟评估
  //到父运算符。
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def needCopyResult: Boolean = withReplacement

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")

      // Inline mutable state since not many Sample operations in a task
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampleReplace",
        v => {
          val initSamplerFuncName = ctx.addNewFunction(initSampler,
            s"""
              | private void $initSampler() {
              |   $v = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
              |   java.util.Random random = new java.util.Random(${seed}L);
              |   long randomSeed = random.nextLong();
              |   int loopCount = 0;
              |   while (loopCount < partitionIndex) {
              |     randomSeed = random.nextLong();
              |     loopCount += 1;
              |   }
              |   $v.setSeed(randomSeed);
              | }
           """.stripMargin.trim)
          s"$initSamplerFuncName();"
        }, forceInline = true)

      val samplingCount = ctx.freshName("samplingCount")
      s"""
         | int $samplingCount = $sampler.sample();
         | while ($samplingCount-- > 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    } else {
      val samplerClass = classOf[BernoulliCellSampler[UnsafeRow]].getName
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampler",
        v => s"""
          | $v = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $v.setSeed(${seed}L + partitionIndex);
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() != 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    }
  }
}


/**
 * 范围的物理计划（生成64位数字的范围）。
 */
case class RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends LeafExecNode with CodegenSupport {
  //start开始标志，end结束标志,step中间阶段标识,numSlices切片操作,numElements元素个数
  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(sparkContext.defaultParallelism)
  val numElements: BigInt = range.numElements
  
  //具体输出
  override val output: Seq[Attribute] = range.output
  //排序输出
  override def outputOrdering: Seq[SortOrder] = range.outputOrdering
  //RDD重新排序
  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doCanonicalize(): SparkPlan = {
    RangeExec(range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdd = if (start == end || (start < end ^ 0 < step)) {
      new EmptyRDD[InternalRow](sqlContext.sparkContext)
    } else {
      sqlContext.sparkContext.parallelize(0 until numSlices, numSlices).map(i => InternalRow(i))
    }
    rdd :: Nil
  }
  
  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    val initTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initRange")
    val nextIndex = ctx.addMutableState(CodeGenerator.JAVA_LONG, "nextIndex")

    val value = ctx.freshName("value")
    val ev = ExprCode.forNonNullValue(JavaCode.variable(value, LongType))
    val BigInt = classOf[java.math.BigInteger].getName

    // 内联可变状态，因为任务中的Range操作不多
    val taskContext = ctx.addMutableState("TaskContext", "taskContext",
      v => s"$v = TaskContext.get();", forceInline = true)
    val inputMetrics = ctx.addMutableState("InputMetrics", "inputMetrics",
      v => s"$v = $taskContext.taskMetrics().inputMetrics();", forceInline = true)

    //为了定期更新指标而不会造成性能损失，这个
    //运算符批量生成元素。批处理完成后，度量标准将更新
    //并启动一个新批次。
    //在下面的实现中，内部循环中的代码生成所有值
    //在批处理中，而外部循环中的代码是设置批处理参数和更新
    //指标
    
    //一旦nextIndex == batchEnd，就可以进入下一批了。
    val batchEnd = ctx.addMutableState(CodeGenerator.JAVA_LONG, "batchEnd")

    //此范围运算符仍应生成多少个值。
    val numElementsTodo = ctx.addMutableState(CodeGenerator.JAVA_LONG, "numElementsTodo")

    //下一批中应生成多少个值。
    val nextBatchTodo = ctx.freshName("nextBatchTodo")

    //批处理的默认大小，必须为正整数
    val batchSize = 1000

    val initRangeFuncName = ctx.addNewFunction("initRange",
      s"""
        | private void initRange(int idx) {
        |   $BigInt index = $BigInt.valueOf(idx);
        |   $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
        |   $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
        |   $BigInt step = $BigInt.valueOf(${step}L);
        |   $BigInt start = $BigInt.valueOf(${start}L);
        |   long partitionEnd;
        |
        |   $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
        |   if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $nextIndex = Long.MAX_VALUE;
        |   } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $nextIndex = Long.MIN_VALUE;
        |   } else {
        |     $nextIndex = st.longValue();
        |   }
        |   $batchEnd = $nextIndex;
        |
        |   $BigInt end = index.add($BigInt.ONE).multiply(numElement).divide(numSlice)
        |     .multiply(step).add(start);
        |   if (end.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     partitionEnd = Long.MAX_VALUE;
        |   } else if (end.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     partitionEnd = Long.MIN_VALUE;
        |   } else {
        |     partitionEnd = end.longValue();
        |   }
        |
        |   $BigInt startToEnd = $BigInt.valueOf(partitionEnd).subtract(
        |     $BigInt.valueOf($nextIndex));
        |   $numElementsTodo  = startToEnd.divide(step).longValue();
        |   if ($numElementsTodo < 0) {
        |     $numElementsTodo = 0;
        |   } else if (startToEnd.remainder(step).compareTo($BigInt.valueOf(0L)) != 0) {
        |     $numElementsTodo++;
        |   }
        | }
       """.stripMargin)

    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val stopCheck = if (parent.needStopCheck) {
      s"""
         |if (shouldStop()) {
         |  $nextIndex = $value + ${step}L;
         |  $numOutput.add($localIdx + 1);
         |  $inputMetrics.incRecordsRead($localIdx + 1);
         |  return;
         |}
       """.stripMargin
    } else {
      "// shouldStop check is eliminated"
    }
    val loopCondition = if (limitNotReachedChecks.isEmpty) {
      "true"
    } else {
      limitNotReachedChecks.mkString(" && ")
    }

    // Range处理的概述。
    //
    //对于每个分区，Range任务需要从分区start（包括）开始生成记录
    //结束（独家）为了获得更好的性能，我们将分区范围分成批次，并且
    //使用2个循环来生成数据。外部while循环用于迭代批次和内部
    // for循环用于迭代批处理中的记录。
    //
    // `nextIndex`跟踪将要消耗和初始化的下一条记录的索引
    //分区开始 `batchEnd`跟踪当前批次的结束索引，已初始化
    //使用`nextIndex`。在外部循环中，我们首先检查`nextIndex == batchEnd`。如果这是真的，
    //这意味着当前批次已被完全消耗，我们将更新`batchEnd`来处理
    //下一批 如果`batchEnd`到达分区结束，则退出外循环。最后我们进入了
    //内循环 注意，当我们进入内部循环时，`nextIndex`必须与之不同
    // `batchEnd`，否则我们已经退出外循环。
    //
    //内部循环从0迭代到`localEnd`，由...计算
    // `（batchEnd  -  nextIndex）/ step`。由于`batchEnd`被`nextBatchTodo * step`增加了
    //外部循环，并用`nextIndex`初始化，所以`batchEnd  -  nextIndex`总是如此
    //可以被`step`整除 在每次迭代期间，`nextIndex`增加`step`，并结束
    //当内循环结束时，等于`batchEnd`。
    //
    //如果查询生成了至少一个结果行，则内部循环可以被中断
    //我们不会缓冲太多结果行并浪费内存。可以中断内循环，
    //因为`nextIndex`将在中断之前更新。


    s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   $initRangeFuncName(partitionIndex);
      | }
      |
      | while ($loopCondition) {
      |   if ($nextIndex == $batchEnd) {
      |     long $nextBatchTodo;
      |     if ($numElementsTodo > ${batchSize}L) {
      |       $nextBatchTodo = ${batchSize}L;
      |       $numElementsTodo -= ${batchSize}L;
      |     } else {
      |       $nextBatchTodo = $numElementsTodo;
      |       $numElementsTodo = 0;
      |       if ($nextBatchTodo == 0) break;
      |     }
      |     $batchEnd += $nextBatchTodo * ${step}L;
      |   }
      |
      |   int $localEnd = (int)(($batchEnd - $nextIndex) / ${step}L);
      |   for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
      |     long $value = ((long)$localIdx * ${step}L) + $nextIndex;
      |     ${consume(ctx, Seq(ev))}
      |     $stopCheck
      |   }
      |   $nextIndex = $batchEnd;
      |   $numOutput.add($localEnd);
      |   $inputMetrics.incRecordsRead($localEnd);
      |   $taskContext.killTaskIfInterrupted();
      | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex { (i, _) =>
        val partitionStart = (i * numElements) / numSlices * step + start
        val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
        def getSafeMargin(bi: BigInt): Long =
          if (bi.isValidLong) {
            bi.toLong
          } else if (bi > 0) {
            Long.MaxValue
          } else {
            Long.MinValue
          }
        val safePartitionStart = getSafeMargin(partitionStart)
        val safePartitionEnd = getSafeMargin(partitionEnd)
        val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + LongType.defaultSize
        val unsafeRow = UnsafeRow.createFromByteArray(rowSize, 1)
        val taskContext = TaskContext.get()

        val iter = new Iterator[InternalRow] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false
          private[this] val inputMetrics = taskContext.taskMetrics().inputMetrics

          override def hasNext =
            if (!overflow) {
              if (step > 0) {
                number < safePartitionEnd
              } else {
                number > safePartitionEnd
              }
            } else false

          override def next() = {
            val ret = number
            number += step
            if (number < ret ^ step < 0) {
              // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
              // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
              // back, we are pretty sure that we have an overflow.
              overflow = true
            }

            numOutputRows += 1
            inputMetrics.incRecordsRead(1)
            unsafeRow.setLong(0, ret)
            unsafeRow
          }
        }
        new InterruptibleIterator(taskContext, iter)
      }
  }

  override def simpleString(maxFields: Int): String = {
    s"Range ($start, $end, step=$step, splits=$numSlices)"
  }
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 *
 * If we change how this is implemented physically, we'd need to update
 * [[org.apache.spark.sql.catalyst.plans.logical.Union.maxRowsPerPartition]].
 */
case class UnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))
}

/**
 * Physical plan for returning a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.  If a larger number of partitions
 * is requested, it will stay at the current number of partitions.
 *
 * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
 * this may result in your computation taking place on fewer nodes than
 * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
 * you see ShuffleExchange. This will add a shuffle step, but means the
 * current upstream partitions will be executed in parallel (per whatever
 * the current partitioning is).
 */
case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (numPartitions == 1 && child.execute().getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new CoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      child.execute().coalesce(numPartitions, shuffle = false)
    }
  }
}

object CoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[InternalRow](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}

/**
 * Parent class for different types of subquery plans
 */
abstract class BaseSubqueryExec extends SparkPlan {
  def name: String
  def child: SparkPlan

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 *子查询的物理计划
 */
case class SubqueryExec(name: String, child: SparkPlan)
  extends BaseSubqueryExec with UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"))

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
     // relationFuture用于“doExecute”。因此，我们可以在这里正确获取执行ID。
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      //这将在另一个线程中运行。设置执行ID，以便我们可以连接这些作业正确执行
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val beforeCollect = System.nanoTime()
        //请注意，我们使用.executeCollect（），因为我们不想将数据转换为Scala类型
        val rows: Array[InternalRow] = child.executeCollect()
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        rows
      }
    }(SubqueryExec.executionContext)
  }

  protected override def doCanonicalize(): SparkPlan = {
    SubqueryExec("Subquery", child.canonicalized)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }
}

object SubqueryExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}

/**
 * A wrapper for reused [[BaseSubqueryExec]].
 */
case class ReusedSubqueryExec(child: BaseSubqueryExec)
  extends BaseSubqueryExec with LeafExecNode {

  override def name: String = child.name

  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = child.canonicalized
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doPrepare(): Unit = child.prepare()

  protected override def doExecute(): RDD[InternalRow] = child.execute()

  override def executeCollect(): Array[InternalRow] = child.executeCollect()
}
