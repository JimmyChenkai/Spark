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

import scala.collection.mutable.HashMap

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

trait DataSourceScanExec extends LeafExecNode with CodegenSupport {
  val relation: BaseRelation
  val tableIdentifier: Option[TableIdentifier]

  protected val nodeNamePrefix: String = ""

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // 描述此扫描的更多详细信息的元数据。
  protected def metadata: Map[String, String]

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), 100)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }

  /**
   *在不指定编辑规则的情况下调用redactString（）的简写
   */
  private def redact(text: String): String = {
    Utils.redact(sqlContext.sessionState.conf.stringRedactionPattern, text)
  }
}


/**
 * 物理计划节点，用于扫描关系中的数据。
 */
case class RowDataSourceScanExec(
    fullOutput: Seq[Attribute],
    requiredColumnsIndex: Seq[Int],
    filters: Set[Filter],
    handledFilters: Set[Filter],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with InputRDDCodegen {

  def output: Seq[Attribute] = requiredColumnsIndex.map(fullOutput)

  //懒加载机制的 指标  
  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
  //执行开始入口
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    //map函数中的重新分区 
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map( r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }

  //输入可以是InternalRow，必须转换为UnsafeRows。
  override protected val createUnsafeProjection: Boolean = true

  override def inputRDD: RDD[InternalRow] = rdd
  //元数据信息
  override val metadata: Map[String, String] = {
    val markedFilters = for (filter <- filters) yield {
      if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
    }
    Map(
      "ReadSchema" -> output.toStructType.catalogString,
      "PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
  }

  //在规范化时，不要关心`rdd`和`tableIdentifier`。
  override def doCanonicalize(): SparkPlan =
    copy(
      fullOutput.map(QueryPlan.normalizeExprId(_, fullOutput)),
      rdd = null,
      tableIdentifier = None)
}

/**
 * 用于从HadoopFsRelations扫描数据的物理计划节点。
 *
 * @param  relation基于文件的扫描关系。
 * @param  输出扫描的输出属性，包括数据属性和分区属性。
 * @param  requiredSchema底层关系的必需模式，不包括分区列。
 * @param  partitionFilters用于分区修剪的谓词。
 * @param  optionalBucketSet用于铲斗修剪的铲斗ID
 * @param  dataFilters在非分区列上过滤。
 * Metastore中表的@param  tableIdentifier标识符。
 */
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan  {

  //请注意，一些引用基于文件的关系的val是故意的
  //这样该计划也可以在执行者方面进行规范化。见SPARK-23731。
  override lazy val supportsBatch: Boolean = {
    relation.fileFormat.supportBatch(relation.sparkSession, schema)
  }

  private lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   *发送驱动程序端指标。在调用此函数之前，selectedPartitions有
   *已初始化。有关详细信息，请参阅SPARK-26327。
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  @transient private lazy val selectedPartitions: Seq[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret = relation.location.listFiles(partitionFilters, dataFilters)
    driverMetrics("numFiles") = ret.map(_.files.size.toLong).sum
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }

  /**
   * [[ partitionFilters ]]可以包含子查询，其结果仅在运行时可用
   * 在规划期间，应使用此方法保护访问[[ selectedPartitions ]]
   */
  private def hasPartitionsAvailableAtRunTime: Boolean = {
    partitionFilters.exists(ExecSubqueryExpression.hasSubquery)
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec match {
      case Some(spec) =>
        // For bucketed columns:
        // -----------------------
        // `HashPartitioning` would be used only when:
        // 1. ALL the bucketing columns are being read from the table
        //
        // For sorted columns:
        // ---------------------
        // Sort ordering should be used when ALL these criteria's match:
        // 1. `HashPartitioning` is being used
        // 2. A prefix (or all) of the sort columns are being read from the table.
        //
        // Sort ordering would be over the prefix subset of `sort columns` being read
        // from the table.
        // eg.
        // Assume (col0, col2, col3) are the columns read from the table
        // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
        // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
        // above

        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          val partitioning = HashPartitioning(bucketColumns, spec.numBuckets)
          val sortColumns =
            spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)

          val sortOrder = if (sortColumns.nonEmpty && !hasPartitionsAvailableAtRunTime) {
            //在分组的情况下，可能有多个文件属于
            //给定关系中的相同存储桶 每个文件都是本地排序的
            //但是这些组合在一起的文件不是全局排序的。鉴于，
            //即使关系已设置排序列，RDD分区也不会排序
            //当前的解决方案是检查所有存储桶中是否包含单个文件

            val files = selectedPartitions.flatMap(partition => partition.files)
            val bucketToFilesGrouping =
              files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
            val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

            if (singleFilePartitions) {
              // TODO目前Spark不支持按降序编写列排序
              //所以使用升序。这可以在将来修复
              sortColumns.map(attribute => SortOrder(attribute, Ascending))
            } else {
              Nil
            }
          } else {
            Nil
          }
          (partitioning, sortOrder)
        } else {
          (UnknownPartitioning(0), Nil)
        }
      case _ =>
        (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsBatch.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)
    val withOptPartitionCount = if (relation.partitionSchemaOption.isDefined &&
      !hasPartitionsAvailableAtRunTime) {
      metadata + ("PartitionCount" -> selectedPartitions.size.toString)
    } else {
      metadata
    }

    val withSelectedBucketsCount = relation.bucketSpec.map { spec =>
      val numSelectedBuckets = optionalBucketSet.map { b =>
        b.cardinality()
      } getOrElse {
        spec.numBuckets
      }
      withOptPartitionCount + ("SelectedBucketsCount" ->
        s"$numSelectedBuckets out of ${spec.numBuckets}")
    } getOrElse {
      withOptPartitionCount
    }

    withSelectedBucketsCount
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  //入口出  
  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      //在回退的情况下，这个批量扫描应该永远不会失败，因为：
      // 1）仅支持基本类型
      // 2）列数应小于spark.sql.codegen.maxFields
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")

      if (needsUnsafeRowConversion) {
        inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
          val proj = UnsafeProjection.create(schema)
          proj.initialize(index)
          iter.map( r => {
            numOutputRows += 1
            proj(r)
          })
        }
      } else {
        inputRDD.map { r =>
          numOutputRows += 1
          r
        }
      }
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   *为分段读取创建RDD。
   *此函数的非分块变体是[[ createNonBucketedReadRDD ]]。
   *
   *算法非常简单：返回的每个RDD分区都应包含所有文件
   *具有来自所有给定Hive分区的相同存储桶ID。
   *
   * @param  bucketSpec的分组规格。
   * @param  readFile读取每个（部分）文件的函数。
   * @param  selectedPartitions Hive风格的分区是读取的一部分。
   * @param  fsRelation [[ HadoopFsRelation ]]与读取相关联。
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Nil))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   *为非分段读取创建RDD。
   *此函数的分块变体是[[ createBucketedReadRDD ]]。
   *
   * @param  readFile读取每个（部分）文件的函数。
   * @param  selectedPartitions Hive风格的分区是读取的一部分。
   * @param  fsRelation [[ HadoopFsRelation ]]与读取相关联。
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  override def doCanonicalize(): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExprId(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(partitionFilters, output),
      optionalBucketSet,
      QueryPlan.normalizePredicates(dataFilters, output),
      None)
  }
}
