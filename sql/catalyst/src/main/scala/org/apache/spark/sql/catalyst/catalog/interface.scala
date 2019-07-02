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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.time.ZoneOffset
import java.util.Date

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


/**
 * 专门定义catalog方法函数的
 *
 * @param identifier 方法名称
 * @param className 类全名称, 例如. "org.apache.spark.util.MyFunc"
 * @param resources 方法是有资源类型集合
 */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource])


/**
 * 存储格式化，用来描述一个分区一个表示如何存储的
 */
case class CatalogStorageFormat(
    locationUri: Option[URI],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
    properties: Map[String, String]) {
  //转为string类型
  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("Storage(", ", ", ")")
  }

  //转为LinkedHashMap
  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    locationUri.foreach(l => map.put("Location", l.toString))
    serde.foreach(map.put("Serde Library", _))
    inputFormat.foreach(map.put("InputFormat", _))
    outputFormat.foreach(map.put("OutputFormat", _))
    if (compressed) map.put("Compressed", "")
    CatalogUtils.maskCredentials(properties) match {
      case props if props.isEmpty => // No-op
      case props =>
        map.put("Storage Properties", props.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]"))
    }
    map
  }
}

object CatalogStorageFormat {
  /** 默认值和副本空存储格式*/
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
}

/**
 * Hive类型的分区在catalog中的定义
 *
 * @param spec 按列名索引的分区规范值
 * @param storage 分区存储格式化
 * @param parameters 分区一些参数
 * @param createTime 创建分区时间 精确到毫秒
 * @param lastAccessTime 最近一次使用时间 精确到毫秒
 * @param stats 统计信息 (多少行，总量是多大等)
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty,
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    stats: Option[CatalogStatistics] = None) {

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    map.put("Partition Values", s"[$specString]")
    map ++= storage.toLinkedHashMap
    if (parameters.nonEmpty) {
      map.put("Partition Parameters", s"{${parameters.map(p => p._1 + "=" + p._2).mkString(", ")}}")
    }
    map.put("Created Time", new Date(createTime).toString)
    val lastAccess = {
      if (-1 == lastAccessTime) "UNKNOWN" else new Date(lastAccessTime).toString
    }
    map.put("Last Access", lastAccess)
    stats.foreach(s => map.put("Partition Statistics", s.simpleString))
    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogPartition(\n\t", "\n\t", ")")
  }

  /** 目录表分区的可读字符串表示形式。*/
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }

  /** 返回分区位置（假定已指定）。 */
  def location: URI = storage.locationUri.getOrElse {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    throw new AnalysisException(s"Partition [$specString] did not specify locationUri")
  }

  /**
   * 根据分区结构，返回一行用来描述表示分区每个信息的
   */
  def toRow(partitionSchema: StructType, defaultTimeZondId: String): InternalRow = {
    val caseInsensitiveProperties = CaseInsensitiveMap(storage.properties)
    val timeZoneId = caseInsensitiveProperties.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
    InternalRow.fromSeq(partitionSchema.map { field =>
      val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
        null
      } else {
        spec(field.name)
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
  }
}


/**
 * 用来表示分桶信息的 类
 * 分桶是把可分解的数据集合拆分为方便操作管理的小子集, 
 * 桶的数量是固定的，所以不会随数据波动
 *
 * @param numBuckets 分桶数量
 * @param bucketColumnNames 用来生成桶ID的列名
 * @param sortColumnNames 用来在每个桶内对数据排序的列
 */
case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String]) {
    //定义sqlconf
  def conf: SQLConf = SQLConf.get
  //如果分桶数量小于0，或者大于配置文件里面指定的最大分桶个数
  //则抛出异常
  if (numBuckets <= 0 || numBuckets > conf.bucketingMaxBuckets) {
    throw new AnalysisException(
      s"Number of buckets should be greater than 0 but less than or equal to " +
        s"bucketing.maxBuckets (`${conf.bucketingMaxBuckets}`). Got `$numBuckets`")
  }
  //重写toSting方法
  override def toString: String = {
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumnNames.nonEmpty) {
      s", sort columns: [${sortColumnNames.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $bucketString$sortString"
  }
  //转为LinkedHashMap
  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    mutable.LinkedHashMap[String, String](
      "Num Buckets" -> numBuckets.toString,
      "Bucket Columns" -> bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]"),
      "Sort Columns" -> sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    )
  }
}

/**
 * catalog中表的定义类
 *
 * 注意，hive的元存储还跟踪倾斜列。 
 * 我们应该考虑在将来，一旦我们对如何处理倾斜列有了更好的理解。
 *
 * @param provider 表存储类型, liru . parquet, json等， 如果是视图就为None,               
 * @param unsupportedFeatures 一个string类型集合，主要是用来描述底层表特征的信息的，sparksql 暂且不支持
 * @param tracksPartitionsInCatalog 表分区信息是否存在catalog中，如果没有需要自动根据文件结构来推断
 * @param schemaPreservesCase 是否表结构是大小写敏感的，当使用hive元数据时候置位false，
 *                            如果无法判断是否大小写敏感，会在查询时候去推断，前提是配置过了
 * @param ignoredProperties 是基础表使用的表属性列表Spark SQL忽略
 * @param createVersion 当创建表元数据时候，记录spark版本. 默认是空字符串
 *                      我们期望这个值来自catalog或者调用ExternalCatalog.createTable。 对于视图来说，这个值是空值
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    createVersion: String = "",
    properties: Map[String, String] = Map.empty,
    stats: Option[CatalogStatistics] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty,
    tracksPartitionsInCatalog: Boolean = false,
    schemaPreservesCase: Boolean = true,
    ignoredProperties: Map[String, String] = Map.empty,
    viewOriginalText: Option[String] = None) {

  import CatalogTable._

  /**
   * 表分区列结构
   */
  def partitionSchema: StructType = {
    val partitionFields = schema.takeRight(partitionColumnNames.length)
    assert(partitionFields.map(_.name) == partitionColumnNames)

    StructType(partitionFields)
  }

  /**
   * 表数据列结构
   */
  def dataSchema: StructType = {
    val dataFields = schema.dropRight(partitionColumnNames.length)
    StructType(dataFields)
  }

  /** 返回指定此表所属的数据库（假定它存在）。 */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** 返回表位置（假定已指定）。*/
  def location: URI = storage.locationUri.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify locationUri")
  }

  /** 如果指定了数据库，则返回此表的完全限定名。 */
  def qualifiedName: String = identifier.unquotedString

  /**
   * 返回用于解析视图的默认数据库名称, 
   * 如果编目表不是视图或由旧版本的Spark（2.2.0之前）创建，则应为“None”。
   */
  def viewDefaultDatabase: Option[String] = properties.get(VIEW_DEFAULT_DATABASE)

  /**
   * 返回用于查询视图的默认数据库名称, the column names are used to
   * 如果编目表不是视图或由旧版本的Spark（2.2.0之前）创建，则应为“None”。
   */
  def viewQueryColumnNames: Seq[String] = {
    for {
      numCols <- properties.get(VIEW_QUERY_OUTPUT_NUM_COLUMNS).toSeq
      index <- 0 until numCols.toInt
    } yield properties.getOrElse(
      s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index",
      throw new AnalysisException("Corrupted view query output column names in catalog: " +
        s"$numCols parts expected, but part $index is missing.")
    )
  }

  /** 语法糖更新“存储”中的字段。 */
  def withNewStorage(
      locationUri: Option[URI] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
  }

  //转为LinkedHashMap
  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")

    identifier.database.foreach(map.put("Database", _))
    map.put("Table", identifier.table)
    if (owner != null && owner.nonEmpty) map.put("Owner", owner)
    map.put("Created Time", new Date(createTime).toString)
    map.put("Last Access", new Date(lastAccessTime).toString)
    map.put("Created By", "Spark " + createVersion)
    map.put("Type", tableType.name)
    provider.foreach(map.put("Provider", _))
    bucketSpec.foreach(map ++= _.toLinkedHashMap)
    comment.foreach(map.put("Comment", _))
    if (tableType == CatalogTableType.VIEW) {
      viewText.foreach(map.put("View Text", _))
      viewOriginalText.foreach(map.put("View Original Text", _))
      viewDefaultDatabase.foreach(map.put("View Default Database", _))
      if (viewQueryColumnNames.nonEmpty) {
        map.put("View Query Output Columns", viewQueryColumnNames.mkString("[", ", ", "]"))
      }
    }

    if (properties.nonEmpty) map.put("Table Properties", tableProperties)
    stats.foreach(s => map.put("Statistics", s.simpleString))
    map ++= storage.toLinkedHashMap
    if (tracksPartitionsInCatalog) map.put("Partition Provider", "Catalog")
    if (partitionColumnNames.nonEmpty) map.put("Partition Columns", partitionColumns)
    if (schema.nonEmpty) map.put("Schema", schema.treeString)

    map
  }
 //重写toString方法
  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("CatalogTable(\n", "\n", ")")
  }

  /** 目录表的可读字符串表示形式。 */
  def simpleString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }
}

object CatalogTable {
  val VIEW_DEFAULT_DATABASE = "view.default.database"
  val VIEW_QUERY_OUTPUT_PREFIX = "view.query.out."
  val VIEW_QUERY_OUTPUT_NUM_COLUMNS = VIEW_QUERY_OUTPUT_PREFIX + "numCols"
  val VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX = VIEW_QUERY_OUTPUT_PREFIX + "col."
}

/**
 * 这类统计信息在[[CatalogTable]]中用于与元存储交互。
 * 我们在这里定义这个新类而不是直接使用[[Statistics]]，因为目录中没有属性的概念。
 */
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, CatalogColumnStat] = Map.empty) {

  /**
   * 把[[CatalogStatistics]] 转为 [[Statistics]], 并将列状态与基于属性的列名称。
   */
  def toPlanStats(planOutput: Seq[Attribute], cboEnabled: Boolean): Statistics = {
    if (cboEnabled && rowCount.isDefined) {
      val attrStats = AttributeMap(planOutput
        .flatMap(a => colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType))))
      // 将大小估计为行数*行大小。
      val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
      Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
    } else {
     //当禁用cbo或表没有其他统计信息时，我们只应用大小
     //估计策略，只在统计信息中传播sizeInBytes。
      Statistics(sizeInBytes = sizeInBytes)
    }
  }

  /** CatalogStatistics的可读字符串表示。*/
  def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString"
  }
}

/**
 * 在[[CatalogTable]]中，列的此类统计信息用于与元存储交互。
 */
case class CatalogColumnStat(
    distinctCount: Option[BigInt] = None,
    min: Option[String] = None,
    max: Option[String] = None,
    nullCount: Option[BigInt] = None,
    avgLen: Option[Long] = None,
    maxLen: Option[Long] = None,
    histogram: Option[Histogram] = None,
    version: Int = CatalogColumnStat.VERSION) {

  /**
   * 返回可用于序列化列状态的从字符串到字符串的映射。
   * 键是列的名称和字段的名称（例如“colname.distinctcount”），
   * 值是值的字符串表示形式。
   * 最小/最大值存储为字符串。它们可以使用反序列化
   * [[CatalogColumnStat.FromExternalString]]。
   *
   * 作为协议的一部分，返回的映射始终包含一个名为“version”的键。
   * 任何为空（无）的字段都不会出现在映射中。
   */
  def toMap(colName: String): Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(s"${colName}.${CatalogColumnStat.KEY_VERSION}", CatalogColumnStat.VERSION.toString)
    distinctCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_DISTINCT_COUNT}", v.toString)
    }
    nullCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_NULL_COUNT}", v.toString)
    }
    avgLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_AVG_LEN}", v.toString) }
    maxLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_LEN}", v.toString) }
    min.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MIN_VALUE}", v) }
    max.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_VALUE}", v) }
    histogram.foreach { h =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_HISTOGRAM}", HistogramSerializer.serialize(h))
    }
    map.toMap
  }

  /** 把 [[CatalogColumnStat]] 转为 [[ColumnStat]]. */
  def toPlanStat(
      colName: String,
      dataType: DataType): ColumnStat =
    ColumnStat(
      distinctCount = distinctCount,
      min = min.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      max = max.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      nullCount = nullCount,
      avgLen = avgLen,
      maxLen = maxLen,
      histogram = histogram,
      version = version)
}

object CatalogColumnStat extends Logging {

  // 用于序列化CatalogColumnStat的字符串键列表
  val KEY_VERSION = "version"
  private val KEY_DISTINCT_COUNT = "distinctCount"
  private val KEY_MIN_VALUE = "min"
  private val KEY_MAX_VALUE = "max"
  private val KEY_NULL_COUNT = "nullCount"
  private val KEY_AVG_LEN = "avgLen"
  private val KEY_MAX_LEN = "maxLen"
  private val KEY_HISTOGRAM = "histogram"

  val VERSION = 2

  private def getTimestampFormatter(): TimestampFormatter = {
    TimestampFormatter(format = "yyyy-MM-dd HH:mm:ss.SSSSSS", zoneId = ZoneOffset.UTC)
  }

  /**
   * 从数据类型的字符串表示形式转换为相应的Catalyst数据类型。
   */
  def fromExternalString(s: String, name: String, dataType: DataType, version: Int): Any = {
    dataType match {
      case BooleanType => s.toBoolean
      case DateType if version == 1 => DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(s))
      case DateType => DateFormatter().parse(s)
      case TimestampType if version == 1 =>
        DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
      case TimestampType => getTimestampFormatter().parse(s)
      case ByteType => s.toByte
      case ShortType => s.toShort
      case IntegerType => s.toInt
      case LongType => s.toLong
      case FloatType => s.toFloat
      case DoubleType => s.toDouble
      case _: DecimalType => Decimal(s)
      // 此版本的spark不使用min/max作为二进制/字符串类型，因此我们忽略它。
      case BinaryType | StringType => null
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column $name of data type: $dataType.")
    }
  }

  /**
   * 
   * 将给定值从Catalyst数据类型转换为外部数据类型的字符串表示形式。
   */
  def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateFormatter().format(v.asInstanceOf[Int])
      case TimestampType => getTimestampFormatter().format(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      // 此版本的spark不使用min/max作为二进制/字符串类型，因此我们忽略它。
      case _ =>
        throw new AnalysisException("Column statistics serialization is not supported for " +
          s"column $colName of data type: $dataType.")
    }
    externalValue.toString
  }


  /**
   * 创建一个 [[CatalogColumnStat]]根据传入的map
   * 这用于从一些外部存储中反序列化列状态。
   * 序列化端在中定义 [[CatalogColumnStat.toMap]].
   */
  def fromMap(
    table: String,
    colName: String,
    map: Map[String, String]): Option[CatalogColumnStat] = {

    try {
      Some(CatalogColumnStat(
        distinctCount = map.get(s"${colName}.${KEY_DISTINCT_COUNT}").map(v => BigInt(v.toLong)),
        min = map.get(s"${colName}.${KEY_MIN_VALUE}"),
        max = map.get(s"${colName}.${KEY_MAX_VALUE}"),
        nullCount = map.get(s"${colName}.${KEY_NULL_COUNT}").map(v => BigInt(v.toLong)),
        avgLen = map.get(s"${colName}.${KEY_AVG_LEN}").map(_.toLong),
        maxLen = map.get(s"${colName}.${KEY_MAX_LEN}").map(_.toLong),
        histogram = map.get(s"${colName}.${KEY_HISTOGRAM}").map(HistogramSerializer.deserialize),
        version = map(s"${colName}.${KEY_VERSION}").toInt
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to parse column statistics for column ${colName} in table $table", e)
        None
    }
  }
}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val VIEW = new CatalogTableType("VIEW")
}


/**
 * catalog中一个数据定义
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: URI,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * 表分区的规范。将列名映射到列值。
   */
  type TablePartitionSpec = Map[String, String]

  /**
   * 初始化空规范。
   */
  lazy val emptyTablePartitionSpec: TablePartitionSpec = Map.empty[String, String]
}

/**
 * 表关系的占位符，在分析期间将被“logicalrelation”或“hivetablerelation”等具体关系替换。
 */
case class UnresolvedCatalogRelation(tableMeta: CatalogTable) extends LeafNode {
  assert(tableMeta.identifier.database.isDefined)
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
}

/**
 * 表示配置单元表的“logicalplan”。
 *
 * TODO: 在我们完全将hive作为数据源后删除此项。
 */
case class HiveTableRelation(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference]) extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(tableMeta.partitionSchema.sameType(partitionCols.toStructType))
  assert(tableMeta.dataSchema.sameType(dataCols.toStructType))

  // 分区列应始终出现在数据列之后。
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def doCanonicalize(): HiveTableRelation = copy(
    tableMeta = tableMeta.copy(
      storage = CatalogStorageFormat.empty,
      createTime = -1
    ),
    dataCols = dataCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index))
    },
    partitionCols = partitionCols.zipWithIndex.map {
      case (attr, index) => attr.withExprId(ExprId(index + dataCols.length))
    }
  )

  override def computeStats(): Statistics = {
    tableMeta.stats.map(_.toPlanStats(output, conf.cboEnabled)).getOrElse {
      throw new IllegalStateException("table stats must be specified.")
    }
  }

  override def newInstance(): HiveTableRelation = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))
}
