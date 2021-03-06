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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

/**
 * 删除一张数据表的物理执行计划
 */
case class DropTableExec(catalog: TableCatalog, ident: Identifier, ifExists: Boolean)
  extends LeafExecNode {

  override def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      //通过catalog删除一张数据表
      catalog.dropTable(ident)
    } else if (!ifExists) {
      throw new NoSuchTableException(ident)
    }

    sqlContext.sparkContext.parallelize(Seq.empty, 1)
  }

  override def output: Seq[Attribute] = Seq.empty
}
