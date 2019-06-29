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
import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Shell

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression, InterpretedPredicate}

/**
* 这个理解为ExternalCatalogUtils工具类
* 核心作用是做字符转义的一系列通用方法
**/
object ExternalCatalogUtils {
  // 这复制了hive`confvars.defaultPartitionName`的默认值，因为catalyst没有
  // 依赖HIVE
  val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // 以下字符串转义代码主要从hive（o.a.h.h.common.fileutils）复制。
  //////////////////////////////////////////////////////////////////////////////////////////////////

  //字符转义类
  //这里列举指定的字符集合clist
  val charToEscape = {
    val bitSet = new java.util.BitSet(128)

    /**
     * ascii 01-1f是需要转义的HTTP控制字符
     * \u000A and \u000D are \n and \r
     */
    val clist = Array(
      '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
      '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
      '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
      '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
      '{', '[', ']', '^')

    clist.foreach(bitSet.set(_))

    if (Shell.WINDOWS) {
      Array(' ', '<', '>', '|').foreach(bitSet.set(_))
    }

    bitSet
  }

  //判断是否需要转义
  def needsEscaping(c: Char): Boolean = {
    c >= 0 && c < charToEscape.size() && charToEscape.get(c)
  }

 //需要转义的路径名称
  def escapePathName(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02X")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }

  //不需要转义路劲名称
  def unescapePathName(path: String): String = {
    val sb = new StringBuilder
    var i = 0

    while (i < path.length) {
      val c = path.charAt(i)
      if (c == '%' && i + 2 < path.length) {
        val code: Int = try {
          Integer.parseInt(path.substring(i + 1, i + 3), 16)
        } catch {
          case _: Exception => -1
        }
        if (code >= 0) {
          sb.append(code.asInstanceOf[Char])
          i += 3
        } else {
          sb.append(c)
          i += 1
        }
      } else {
        sb.append(c)
        i += 1
      }
    }

    sb.toString()
  }

  //生成分区路径
  def generatePartitionPath(
      spec: TablePartitionSpec,
      partitionColumnNames: Seq[String],
      tablePath: Path): Path = {
    val partitionPathStrings = partitionColumnNames.map { col =>
      getPartitionPathString(col, spec(col))
    }
    partitionPathStrings.foldLeft(tablePath) { (totalPath, nextPartPath) =>
      new Path(totalPath, nextPartPath)
    }
  }

  def getPartitionPathString(col: String, value: String): String = {
    val partitionString = if (value == null || value.isEmpty) {
      DEFAULT_PARTITION_NAME
    } else {
      escapePathName(value)
    }
    escapePathName(col) + "=" + partitionString
  }

  def prunePartitionsByFilter(
      catalogTable: CatalogTable,
      inputPartitions: Seq[CatalogTablePartition],
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    if (predicates.isEmpty) {
      inputPartitions
    } else {
      val partitionSchema = catalogTable.partitionSchema
      val partitionColumnNames = catalogTable.partitionColumnNames.toSet

      val nonPartitionPruningPredicates = predicates.filterNot {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      }
      if (nonPartitionPruningPredicates.nonEmpty) {
        throw new AnalysisException("Expected only partition pruning predicates: " +
          nonPartitionPruningPredicates)
      }

      val boundPredicate =
        InterpretedPredicate.create(predicates.reduce(And).transform {
          case att: AttributeReference =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })

      inputPartitions.filter { p =>
        boundPredicate.eval(p.toRow(partitionSchema, defaultTimeZoneId))
      }
    }
  }

  /**
   * Returns true if `spec1` is a partial partition spec w.r.t. `spec2`, e.g. PARTITION (a=1) is a
   * partial partition spec w.r.t. PARTITION (a=1,b=2).
   */
  def isPartialPartitionSpec(
      spec1: TablePartitionSpec,
      spec2: TablePartitionSpec): Boolean = {
    spec1.forall {
      case (partitionColumn, value) => spec2(partitionColumn) == value
    }
  }
}

object CatalogUtils {
  /**
   * Masking credentials in the option lists. For example, in the sql plan explain output
   * for JDBC data sources.
   */
  def maskCredentials(options: Map[String, String]): Map[String, String] = {
    options.map {
      case (key, _) if key.toLowerCase(Locale.ROOT) == "password" => (key, "###")
      case (key, value)
        if key.toLowerCase(Locale.ROOT) == "url" &&
          value.toLowerCase(Locale.ROOT).contains("password") =>
        (key, "###")
      case o => o
    }
  }

  def normalizePartCols(
      tableName: String,
      tableCols: Seq[String],
      partCols: Seq[String],
      resolver: Resolver): Seq[String] = {
    partCols.map(normalizeColumnName(tableName, tableCols, _, "partition", resolver))
  }

  def normalizeBucketSpec(
      tableName: String,
      tableCols: Seq[String],
      bucketSpec: BucketSpec,
      resolver: Resolver): BucketSpec = {
    val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec
    val normalizedBucketCols = bucketColumnNames.map { colName =>
      normalizeColumnName(tableName, tableCols, colName, "bucket", resolver)
    }
    val normalizedSortCols = sortColumnNames.map { colName =>
      normalizeColumnName(tableName, tableCols, colName, "sort", resolver)
    }
    BucketSpec(numBuckets, normalizedBucketCols, normalizedSortCols)
  }

  /**
   * URI转换为字符串。
   * 由于uri.toString不解码该uri，例如将“%25”更改为“%”。
   * 在这里，我们用给定的URI创建一个hadoop路径，并依赖path.toString解码URI
   * @param uri 
   * @return path
   */
  def URIToString(uri: URI): String = {
    new Path(uri).toString
  }

  /**
   * 将字符串转换为URI。
   * 因为new uri（string）不编码字符串，例如将“%”更改为“%25”。
   * 在这里，我们用给定的字符串创建一个hadoop路径，并依赖path.touri
   * 对字符串进行编码
   * @param str path
   * @return the URI 
   */
  def stringToURI(str: String): URI = {
    new Path(str).toUri
  }

  private def normalizeColumnName(
      tableName: String,
      tableCols: Seq[String],
      colName: String,
      colType: String,
      resolver: Resolver): String = {
    tableCols.find(resolver(_, colName)).getOrElse {
      throw new AnalysisException(s"$colType column $colName is not defined in table $tableName, " +
        s"defined table columns are: ${tableCols.mkString(", ")}")
    }
  }
}
