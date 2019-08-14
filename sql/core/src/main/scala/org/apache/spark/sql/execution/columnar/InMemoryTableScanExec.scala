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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

//继承实现了LeafExecNode和ColumnarBatchScan
//参数一 是属性
//参数二 是谓词
//参数三 是内存表之间关系
case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode with ColumnarBatchScan {

  //覆盖 了节点名称    
  override val nodeName: String = {
    relation.cacheBuilder.tableName match {
      case Some(_) =>
        "Scan " + relation.cacheBuilder.cachedName
      case _ =>
        super.nodeName
    }
  }

  //内部子节点    
  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  //覆盖重写了doCanonicalize方法
  //拷贝属性，谓词，关系  
  override def doCanonicalize(): SparkPlan =
    copy(attributes = attributes.map(QueryPlan.normalizeExprId(_, relation.output)),
      predicates = predicates.map(QueryPlan.normalizeExprId(_, relation.output)),
      relation = relation.canonicalized.asInstanceOf[InMemoryRelation])

  override def vectorTypes: Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  /**
   *如果为true，则从ColumnarBatch中的ColumnVector获取数据，这通常更快。
   *如果为false，则从CachedBatch从UnsafeRow构建获取数据
   */
  override val supportsBatch: Boolean = {
    //在初始实施中，为了便于审查
    //仅支持原始数据类型，字段数小于wholeStageMaxNumFields
    conf.cacheVectorizedReaderEnabled && relation.schema.fields.forall(f => f.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    }) && !WholeStageCodegenExec.isTooManyFields(conf, relation.schema)
  }

  // TODO:重温这个。如果输出数据是行，我们总是关闭整个阶段的codegen吗？
  override def supportCodegen: Boolean = supportsBatch
  
  //列
  private val columnIndices =
    attributes.map(a => relation.output.map(o => o.exprId).indexOf(a.exprId)).toArray
  
  //关系之间结构
  private val relationSchema = relation.schema.toArray

  //批处理列结构    
  private lazy val columnarBatchSchema = new StructType(columnIndices.map(i => relationSchema(i)))

  /**
    * 创建和解压缩列    
    * param1 cachedColumnarBatch
    * param2 offHeapColumnVectorEnabled
    * return ColumnarBatch
    **/
  private def createAndDecompressColumn(
      cachedColumnarBatch: CachedBatch,
      offHeapColumnVectorEnabled: Boolean): ColumnarBatch = {
    
    //cachedColumnarBatch行数
    val rowCount = cachedColumnarBatch.numRows
    val taskContext = Option(TaskContext.get())
    //列集合，offHeapColumnVectorEnabled是false或者taskContext空
    //则开始分配列在JVM堆中
    //否则是分配列在JVM堆外  
    val columnVectors = if (!offHeapColumnVectorEnabled || taskContext.isEmpty) {
      OnHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    } else {
      OffHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    }
    val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])
    columnarBatch.setNumRows(rowCount)

    //列解压缩处理  
    for (i <- attributes.indices) {
      ColumnAccessor.decompress(
        cachedColumnarBatch.buffers(columnIndices(i)),
        columnarBatch.column(i).asInstanceOf[WritableColumnVector],
        columnarBatchSchema.fields(i).dataType, rowCount)
    }
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => columnarBatch.close()))
    columnarBatch
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    val buffers = filteredCachedBatches()
    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    if (supportsBatch) {
      // HACK ALERT：这实际上是一个RDD [ColumnarBatch]。
      //我们在这里利用Scala的类型擦除来传递这些批次。
      buffers
        .map(createAndDecompressColumn(_, offHeapColumnVectorEnabled))
        .asInstanceOf[RDD[InternalRow]]
    } else {
      val numOutputRows = longMetric("numOutputRows")

      if (enableAccumulatorsForTest) {
        readPartitions.setValue(0)
        readBatches.setValue(0)
      }

      //在这里使用这些变量来避免整个对象的序列化（如果引用的话）
      //直接）在地图分区封闭中。
      val relOutput: AttributeSeq = relation.output

      filteredCachedBatches().mapPartitionsInternal { cachedBatchIterator =>
       //查找所请求列的序数和数据类型。
        val (requestedColumnIndices, requestedColumnDataTypes) =
          attributes.map { a =>
            relOutput.indexOf(a.exprId) -> a.dataType
          }.unzip

        //更新SQL指标
        val withMetrics = cachedBatchIterator.map { batch =>
          if (enableAccumulatorsForTest) {
            readBatches.add(1)
          }
          numOutputRows += batch.numRows
          batch
        }

        val columnTypes = requestedColumnDataTypes.map {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }.toArray
        val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
        columnarIterator.initialize(withMetrics, columnTypes, requestedColumnIndices.toArray)
        if (enableAccumulatorsForTest && columnarIterator.hasNext) {
          readPartitions.add(1)
        }
        columnarIterator
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def output: Seq[Attribute] = attributes

  private def updateAttribute(expr: Expression): Expression = {
    //可以使用关系的输出修剪属性。
    //例如，relation.output是[id，item]但是此扫描的输出只能是[item]。
    val attrMap = AttributeMap(relation.cachedPlan.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  //缓存版本不会更改原始SparkPlan的outputPartitioning。
  //但是缓存版本可以为输出设置别名，因此我们需要替换输出。
  override def outputPartitioning: Partitioning = {
    relation.cachedPlan.outputPartitioning match {
      case e: Expression => updateAttribute(e).asInstanceOf[Partitioning]
      case other => other
    }
  }

  //缓存版本不会更改原始SparkPlan的outputOrdering。
  //但是缓存版本可以为输出设置别名，因此我们需要替换输出。
  override def outputOrdering: Seq[SortOrder] =
    relation.cachedPlan.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  //保留关系的分区统计信息，因为我们不会序列化关系。
  private val stats = relation.partitionStatistics
  private def statsFor(a: Attribute) = stats.forAttribute(a)

  //目前，仅使用原子类型的统计信息，仅限二进制类型。
  private object ExtractableLiteral {
    def unapply(expr: Expression): Option[Literal] = expr match {
      case lit: Literal => lit.dataType match {
        case BinaryType => None
        case _: AtomicType => Some(lit)
        case _ => None
      }
      case _ => None
    }
  }

  //如果输入表达式不可能，则返回的过滤谓词应该返回false
  //根据收集的有关此分区批次的统计信息评估为“true”。
  @transient lazy val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      //若buildFilter在左右节点都定义了则合并
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, ExtractableLiteral(l)) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(ExtractableLiteral(l), a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case EqualNullSafe(a: AttributeReference, ExtractableLiteral(l)) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualNullSafe(ExtractableLiteral(l), a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, ExtractableLiteral(l)) => statsFor(a).lowerBound < l
    case LessThan(ExtractableLiteral(l), a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
      statsFor(a).lowerBound <= l
    case LessThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
      l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, ExtractableLiteral(l)) => l < statsFor(a).upperBound
    case GreaterThan(ExtractableLiteral(l), a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
      l <= statsFor(a).upperBound
    case GreaterThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
      statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0

    case In(a: AttributeReference, list: Seq[Expression])
      if list.forall(ExtractableLiteral.unapply(_).isDefined) && list.nonEmpty =>
      list.map(l => statsFor(a).lowerBound <= l.asInstanceOf[Literal] &&
        l.asInstanceOf[Literal] <= statsFor(a).upperBound).reduce(_ || _)

    // This is an example to explain how it works, imagine that the id column stored as follows:
    // __________________________________________
    // | Partition ID | lowerBound | upperBound |
    // |--------------|------------|------------|
    // |      p1      |    '1'     |    '9'     |
    // |      p2      |    '10'    |    '19'    |
    // |      p3      |    '20'    |    '29'    |
    // |      p4      |    '30'    |    '39'    |
    // |      p5      |    '40'    |    '49'    |
    // |______________|____________|____________|
    //
    // A filter: df.filter($"id".startsWith("2")).
    // In this case it substr lowerBound and upperBound:
    // ________________________________________________________________________________________
    // | Partition ID | lowerBound.substr(0, Length("2")) | upperBound.substr(0, Length("2")) |
    // |--------------|-----------------------------------|-----------------------------------|
    // |      p1      |    '1'                            |    '9'                            |
    // |      p2      |    '1'                            |    '1'                            |
    // |      p3      |    '2'                            |    '2'                            |
    // |      p4      |    '3'                            |    '3'                            |
    // |      p5      |    '4'                            |    '4'                            |
    // |______________|___________________________________|___________________________________|
    //
    // We can see that we only need to read p1 and p3.
    case StartsWith(a: AttributeReference, ExtractableLiteral(l)) =>
      statsFor(a).lowerBound.substr(0, Length(l)) <= l &&
        l <= statsFor(a).upperBound.substr(0, Length(l))
  }

  //懒加载机制中分区过滤    
  lazy val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            stats.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      //如果无法解析过滤器，那么我们缺少必需的统计信息。
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulatorsForTest: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  //用于测试目的的累加器
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  private def filteredCachedBatches(): RDD[CachedBatch] = {
    //在这里使用这些变量来避免整个对象的序列化（如果直接引用）
    //在地图内部分区封闭。
    val schema = stats.schema
    val schemaIndex = schema.zipWithIndex
    val buffers = relation.cacheBuilder.cachedColumnBuffers

    buffers.mapPartitionsWithIndexInternal { (index, cachedBatchIterator) =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        schema)
      partitionFilter.initialize(index)

      //如果启用了分区批量修剪
      if (inMemoryPartitionPruningEnabled) {
        cachedBatchIterator.filter { cachedBatch =>
          if (!partitionFilter.eval(cachedBatch.stats)) {
            logDebug {
              val statsString = schemaIndex.map { case (a, i) =>
                val value = cachedBatch.stats.get(i, a.dataType)
                s"${a.name}: $value"
              }.mkString(", ")
              s"Skipping partition based on stats $statsString"
            }
            false
          } else {
            true
          }
        }
      } else {
        cachedBatchIterator
      }
    }
  }

  //执行入口出    
  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      inputRDD
    }
  }
}
