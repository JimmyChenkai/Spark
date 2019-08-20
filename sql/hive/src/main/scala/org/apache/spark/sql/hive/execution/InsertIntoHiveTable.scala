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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.HiveClientImpl


/**
 * Command for writing data out to a Hive table.
 *
 * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
 * follow Hive's internal implementations closely, which itself was a mess too). Please don't
 * blame Reynold for this! He was just moving code around!
 *
 * In the future we should converge the write path for Hive with the normal data source write path,
 * as defined in `org.apache.spark.sql.execution.datasources.FileFormatWriter`.
 *
 * @param  表格的元数据。
 * @参数 划分从分区键分隔值（可选）的地图。如果是分区
 * value是可选的，将执行动态分区插入。
 *                  一个例子, `INSERT INTO tbl PARTITION (a=1, b=2) AS ...` would have
 *
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> Some('2'))
 *                  }}}
 *
 *                  and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                  would have
 *
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> None)
 *                  }}}.
 * @param  查询表示要写入的数据的逻辑计划。
 * @param  覆盖覆盖现有表或分区。
 * @param  ifPartitionNotExists如果为true，则仅在分区不存在时写入。
 *仅对静态分区有效。
 */
case class InsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String]) extends SaveAsHiveFile {

  /**
   *将表中的所有行插入Hive。行对象已正确序列化
   *`org.apache.hadoop.hive.serde2.SerDe`和
   *表定义提供的`org.apache.hadoop.mapred.OutputFormat`。
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
     //必须将TableDesc对象传递给RDD.mapPartitions，然后实例化新的序列化程序
     //闭包中的实例，因为在TableDesc时序列化程序不可序列化。
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      //表的类应该是org.apache.hadoop.hive.ql.metadata.Table，因为
      // getOutputFormatClass将使用HiveFileFormatUtils.getOutputFormatSubstitute来
      //替换一些输出格式，例如将SequenceFileOutputFormat替换为
      // HiveSequenceFileOutputFormat。
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val tableLocation = hiveQlTable.getDataLocation
    val tmpLocation = getExternalTmpPath(sparkSession, hadoopConf, tableLocation)

    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tableDesc, tmpLocation, child)
    } finally {
      //尝试删除登台目录和包含文件。如果失败，则文件为
      //由于使用了deleteOnExit，因此预计会在VM的正常终止时被删除。
      deleteExternalTmpPath(hadoopConf)
    }

    //取消缓存此表。
    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    CommandUtils.updateTableStats(sparkSession, table)

    //最好只返回childRdd，这样就可以链接插入操作了，
    //但是现在我们返回一个空列表来简化与hive的兼容性检查
    //不会为插入操作返回任何内容。
    // TODO：将hive兼容性作为规则实现。
    Seq.empty[Row]
  }

  private def processInsert(
      sparkSession: SparkSession,
      externalCatalog: ExternalCatalog,
      hadoopConf: Configuration,
      tableDesc: TableDesc,
      tmpLocation: Path,
      child: SparkPlan): Unit = {
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    //所有分区列名称格式为“<列名1> / <列名2> / ...”
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    //此时，分区映射必须与表的分区列匹配
    if (partitionColumnNames.toSet != partition.keySet) {
      throw new SparkException(
        s"""Requested partitioning does not match the ${table.identifier.table} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionColumnNames.mkString(",")}""".stripMargin)
    }

    //如果存在任何动态分区，则验证分区规范
    if (numDynamicPartitions > 0) {
      //如果未启用动态分区，则报告错误
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      //如果启用了动态分区严格模式但未找到静态分区，则报告错误
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      //如果动态分区后出现任何静态分区，则报告错误
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    table.bucketSpec match {
      case Some(bucketSpec) =>
        //只有当用户不关心维护时，才允许写入bucketed hive表
        //表的分组即。“hive.enforce.bucketing”和“hive.enforce.sorting”都是
        //设置为false
        val enforceBucketingConfig = "hive.enforce.bucketing"
        val enforceSortingConfig = "hive.enforce.sorting"

        val message = s"Output Hive table ${table.identifier} is bucketed but Spark " +
          "currently does NOT populate bucketed output which is compatible with Hive."

        if (hadoopConf.get(enforceBucketingConfig, "true").toBoolean ||
          hadoopConf.get(enforceSortingConfig, "true").toBoolean) {
          throw new AnalysisException(message)
        } else {
          logWarning(message + s" Inserting data anyways since both $enforceBucketingConfig and " +
            s"$enforceSortingConfig are set to false.")
        }
      case _ => //由于表没有bucketing，所以什么都不做
    }

    //分区属性  
    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
      }.asInstanceOf[Attribute]
    }
    
    //保存为hive文件   
    saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = tmpLocation.toString,
      partitionAttributes = partitionAttributes)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle：关闭
        // ifNotExists仅对静态分区有效，请参阅
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle：on
        val oldPart =
          externalCatalog.getPartitionOption(
            table.database,
            table.identifier.table,
            partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-18107：插入覆盖运行比hive-client慢得多。
          //较新的Hive在很大程度上提高了插入覆盖性能。因为Spark使用较旧的Hive
          //版本，我们可能不想每次都赶上新的Hive版本。我们删除了
          //首先进行Hive分区，然后将数据文件加载到Hive分区。
          if (oldPart.nonEmpty && overwrite) {
            oldPart.get.storage.locationUri.foreach { uri =>
              val partitionPath = new Path(uri)
              val fs = partitionPath.getFileSystem(hadoopConf)
              if (fs.exists(partitionPath)) {
                if (!fs.delete(partitionPath, true)) {
                  throw new RuntimeException(
                    "Cannot remove partition directory '" + partitionPath.toString)
                }
                //不要让Hive覆盖操作，因为它比较慢。
                doHiveOverwrite = false
              }
            }
          }

          // inheritTableSpecs设置为true。对于IMPORT查询，它应设置为false
          //当前被视为Hive本机命令。
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
    }
  }
}
