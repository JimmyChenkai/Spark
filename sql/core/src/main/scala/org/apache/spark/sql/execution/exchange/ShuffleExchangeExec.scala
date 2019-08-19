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

package org.apache.spark.sql.execution.exchange

import java.util.Random
import java.util.function.Supplier

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}

/**
 *执行一个将导致所需`newPartitioning`的shuffle。
 */
case class ShuffleExchangeExec(
    var newPartitioning: Partitioning,
    child: SparkPlan,
    @transient coordinator: Option[ExchangeCoordinator]) extends Exchange {

 //注意：序列化/反序列化后，coordinator可以为null，
  //        例如，它在Executor端可以为null
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  override def nodeName: String = {
    val extraInfo = coordinator match {
      case Some(exchangeCoordinator) =>
        s"(coordinator id: ${System.identityHashCode(exchangeCoordinator)})"
      case _ => ""
    }

    val simpleNodeName = "Exchange"
    s"$simpleNodeName$extraInfo"
  }

  override def outputPartitioning: Partitioning = newPartitioning

  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  override protected def doPrepare(): Unit = {
   //如果需要ExchangeCoordinator，我们会注册此Exchange operator
    //当我们准备时，给协调员。确保这一点很重要
    //我们在执行之前注册此运算符而不是注册它
    //在构造函数中，因为我们可能会创建新的实例
    //在转换物理计划时交换运营商
    //（然后ExchangeCoordinator将保存不需要的Exchange的引用）。
    //所以，我们应该在开始执行之前调用registerExchange计划
    coordinator match {
      case Some(exchangeCoordinator) => exchangeCoordinator.registerExchange(this)
      case _ =>
    }
  }

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  /**
   * [[ ShuffleDependency ]]将根据其子行划分
   *`newPartitioning`中定义的分区方案。那些分区
   *返回的ShuffleDependency将是shuffle的输入。
   */
  @transient
  lazy val shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      newPartitioning,
      serializer,
      writeMetrics)
  }

  /**
   *返回表示后混洗数据集的[[ ShuffledRowRDD ]]。
   *此[[ ShuffledRowRDD ]]是基于给定[[ ShuffleDependency ]]和可选项创建的
   *分区开始索引数组。如果定义了此可选数组，则返回
   * [[ ShuffledRowRDD ]]将根据此数组的索引获取pre-shuffle分区。
   */
  private[exchange] def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    //如果提供了一个分区起始索引数组，我们需要使用这个数组
    //创建ShuffledRowRDD。此外，我们需要更新newPartitioning
    //更新后洗牌分区的数量。
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, readMetrics, specifiedPartitionStartIndices)
  }

  /**
   * 缓存创建的ShuffleRowRDD，以便我们可以重用它。
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    //如果多个计划使用此计划，则返回相同的ShuffleRowRDD。
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = coordinator match {
        case Some(exchangeCoordinator) =>
          val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
          assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
          shuffleRDD
        case _ =>
          preparePostShuffleRDD(shuffleDependency)
      }
    }
    cachedShuffleRDD
  }
}

object ShuffleExchangeExec {
  def apply(newPartitioning: Partitioning, child: SparkPlan): ShuffleExchangeExec = {
    ShuffleExchangeExec(newPartitioning, child, coordinator = Option.empty[ExchangeCoordinator])
  }

  /**
   *确定在发送到随机播放之前是否必须防御性地复制记录。
   * Spark的一些shuffle组件将缓冲内存中反序列化的Java对象。该
   * shuffle代码假定对象是不可变的，因此不会执行自己的防御
   *复制。但是，在Spark SQL中，运算符的迭代器返回相同的可变`Row`对象。在
   *为了正确地改变这些运算符的输出，我们需要执行自己的复制
   *在将记录发送到随机播放之前。这种复制很昂贵，所以我们尽量避免它
   * 只要有可能。此方法封装了用于选择何时复制的逻辑。
   *
   *从长远来看，我们可能希望将此逻辑推入核心的shuffle API，以便我们不这样做
   *必须依赖SQL中的核心内部知识。
   *
   *有关此问题的更多讨论，请参见SPARK-2967，SPARK-4479和SPARK-7375。
   *
   * @param  partitioner用于shuffle的分区器
   * @return如果要在洗牌之前复制行，则返回 true，否则返回 false
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    //注意：即使我们只使用分区器的`numPartitions`字段，我们也要求它
    //传递而不是直接传递分区数以防止
    //可以输出使用`numPartitions`分区构造的分区程序的情况
    //更少的分区（例如RangePartitioner）。
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        //如果我们使用原始的SortShuffleManager并且输出分区的数量是
        //足够小，然后Spark将回退到基于散列的shuffle写路径，这就是
        //不缓冲反序列化的记录。
        //请注意，如果我们修复SPARK-6026并删除此旁路，我们将不得不删除此案例。
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550和SPARK-7081扩展基于排序的shuffle来序列化单个记录
        //在排序之前。此优化仅适用于随机播放的情况
        //依赖项不指定聚合器或排序，并且记录序列化程序具有
        //某些属性和分区数量不超过限制。如果这
        //启用优化，我们可以安全地避免复制。
        //
        // Exchange永远不会使用聚合器或密钥排序来配置其ShuffledRDD
        // Spark SQL中的序列化程序总是满足属性，所以我们只需要检查是否
        //分区数超出限制。
        false
      } else {
        // Spark的SortShuffleManager使用`ExternalSorter`来缓冲内存中的记录，所以我们必须这样备份复制
        true
      }
    } else {
      //捕获所有案例以安全地处理任何未来的ShuffleManager实现。
      true
    }
  }

  /**
   *返回[[ ShuffleDependency ]]，它将根据子行的行进行分区
   *`newPartitioning`中定义的分区方案。那些分区
   *返回的ShuffleDependency将是shuffle的输入。
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric])
    : ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          //对于HashPartitioning，分区键已经是我们使用的有效分区ID
          // `HashPartitioning.partitionIdExpression`生成分区键。
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case RangePartitioning(sortingExpressions, numPartitions) =>
        //仅提取用于排序的字段，以避免收集不包含的大字段
        //在RangePartitioner中确定分区边界时影响排序结果
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          //在内部，RangePartitioner在RDD上运行一个作业，用于对要计算的键进行采样
          //分区边界。为了获得准确的样本，我们需要复制可变密钥。
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        //构造对提取的排序键的排序。
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: 处理 BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
         //从随机分区开始，在输出分区之间均匀分配元素。
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // HashPartitioner将按分区数处理`mod`
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection = UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // [SPARK-23207]必须确保生成的RoundRobinPartitioning是确定性的，
      //否则重试任务可能会输出不同的行，从而导致数据丢失。
      //
      //目前我们遵循之前执行本地排序的最直接方式
      //分区
      //
      //请注意，如果新分区只有1个分区，我们不会执行本地排序
      //那种情况下所有输出行都转到同一个分区。
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          //用于比较行哈希码的比较器，它应始终为整数。
          val prefixComparator = PrefixComparators.LONG
          val canUseRadixSort = SQLConf.get.enableRadixSort
          //前缀计算机生成行哈希码作为前缀，所以我们可以减少
          //输入行从a中选择列值时前缀相等的概率
          //范围有限
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            canUseRadixSort)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      //如果我们不对输入进行排序，则循环函数是顺序敏感的。
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }, isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }, isOrderSensitive = isOrderSensitive)
      }
    }

    //现在，我们手动创建一个ShuffleDependency。因为在rddWithPartitionIds中对
    //的形式为（partitionId，row），每个partitionId都在预期的范围内
    // [0，part.numPartitions  -  1]。这个分区是PartitionIdPassthrough。
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))

    dependency
  }

  /**
   * 为SQL 创建一个自定义的[[ ShuffleWriteProcessor ]]，它包装默认的metrics 纪录
   * 用[ SQLShuffleWriteMetricsReporter为[[]]为新记者ShuffleWriteProcessor ]。
   */
  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}
