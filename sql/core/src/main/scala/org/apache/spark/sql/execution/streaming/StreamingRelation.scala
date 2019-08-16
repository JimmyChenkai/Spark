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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.sources.v2.{Table, TableProvider}
import org.apache.spark.sql.sources.v2.reader.streaming.SparkDataStream
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object StreamingRelation {
  def apply(dataSource: DataSource): StreamingRelation = {
    StreamingRelation(
      dataSource, dataSource.sourceInfo.name, dataSource.sourceInfo.schema.toAttributes)
  }
}

/**
 *用于将流[[ DataSource ]] 链接到
 * [[ org.apache.spark.sql.catalyst.plans.logical.LogicalPlan ]]。这仅用于创建
 *一个流[[ org.apache.spark.sql.DataFrame从[[]] org.apache.spark.sql.DataFrameReader ]。
 *它应该用于创建[[ Source ]]并转换为[[ StreamingExecutionRelation ]]时
 *传递给[[ StreamExecution ]]以运行查询。
 */
case class StreamingRelation(dataSource: DataSource, sourceName: String, output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {
  override def isStreaming: Boolean = true
  override def toString: String = sourceName

  //这里没有明智的价值 在执行路径上，这种关系将是
  //用微型计算机换掉了。但是一些数据帧操作（特别是解释）确实起了作用
  //到这个节点幸存的分析。因此，我们使用会话默认值满足LeafNode规定值
  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(dataSource.sparkSession.sessionState.conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))
}

/**
 *用于将数据流[[ Source ]] 链接到a
 * [[ org.apache.spark.sql.catalyst.plans.logical.LogicalPlan ]]。
 */
case class StreamingExecutionRelation(
    source: SparkDataStream,
    output: Seq[Attribute])(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {

  override def otherCopyArgs: Seq[AnyRef] = session :: Nil
  override def isStreaming: Boolean = true
  override def toString: String = source.toString

  //这里没有明智的价值 在执行路径上，这种关系将是
  //用微型计算机换掉了。但是一些数据帧操作（特别是解释）确实起了作用
  //到这个节点幸存的分析。因此，我们使用会话默认值满足LeafNode规定值
  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))(session)
}

//对于源实现的情况，我们必须将V1数据源打包为shim
//连续处理（总是V2）但只有V1微量支持。我们没有
//在读取时知道查询是否是连续的，所以我们需要能够
//重新交换V1关系。
/**
*用于将[[ TableProvider ]] 链接到流式传输
* [[ org.apache.spark.sql.catalyst.plans.logical.LogicalPlan ]]。这仅用于创建
*一个流[[ org.apache.spark.sql.DataFrame从[[]] org.apache.spark.sql.DataFrameReader ]]
*并且应该在转到[[ StreamExecution ]] 之前进行转换。
 */
case class StreamingRelationV2(
    source: TableProvider,
    sourceName: String,
    table: Table,
    extraOptions: CaseInsensitiveStringMap,
    output: Seq[Attribute],
    v1Relation: Option[StreamingRelation])(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {
  override def otherCopyArgs: Seq[AnyRef] = session :: Nil
  override def isStreaming: Boolean = true
  override def toString: String = sourceName

  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(session.sessionState.conf.defaultSizeInBytes)
  )

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))(session)
}

/**
 * 支持[[ StreamingRelation ]]的虚拟物理计划
 * [[ org.apache.spark.sql.Dataset.explain ]]
 */
case class StreamingRelationExec(sourceName: String, output: Seq[Attribute]) extends LeafExecNode {
  override def toString: String = sourceName
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("StreamingRelationExec cannot be executed")
  }
}

object StreamingExecutionRelation {
  def apply(source: Source, session: SparkSession): StreamingExecutionRelation = {
    StreamingExecutionRelation(source, source.schema.toAttributes)(session)
  }
}
