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

import org.apache.spark.sql.catalyst.analysis.{FunctionAlreadyExistsException, NoSuchDatabaseException, NoSuchFunctionException, NoSuchPartitionException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * system catalog（方法，分区，表，数据库）的接口
 * 
 * 这个接口是为非临时的items提供，同时接口的继承类们一定是线程安全的，
 * 由于接口是需要用到多线程。
 * 这是一个外部的catalog因为是需要和外部的系统进行交互
 *
 * 如果数据库不存在的时候，实现接口的类或者方法一定要抛出[[NoSuchDatabaseException]]异常
 */
trait ExternalCatalog {
  import CatalogTypes.TablePartitionSpec

  // --------------------------------------------------------------------------
  // 通用 层面
  // --------------------------------------------------------------------------

  //判断数据库是否存在
  protected def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }
  //判断数据表是否存在
  protected def requireTableExists(db: String, table: String): Unit = {
    if (!tableExists(db, table)) {
      throw new NoSuchTableException(db = db, table = table)
    }
  }
  //判断方法是存在
  protected def requireFunctionExists(db: String, funcName: String): Unit = {
    if (!functionExists(db, funcName)) {
      throw new NoSuchFunctionException(db = db, func = funcName)
    }
  }
  //判断方法不存在
  protected def requireFunctionNotExists(db: String, funcName: String): Unit = {
    if (functionExists(db, funcName)) {
      throw new FunctionAlreadyExistsException(db = db, func = funcName)
    }
  }

  // --------------------------------------------------------------------------
  // 数据库 层面
  // --------------------------------------------------------------------------
  
  //创建数据库，成功就放回true，失败则返回false
  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit
  //删除数据库，成功就返回tru，失败就返回false
  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit

  /**
   * 如果数据存在的话，可以修改哪些已经在`dbDefinition`明确定义过的数据库
   *
   * 注意: 如果实现类中在底层不支持修改某个字段，那么就是禁止的
   */
  def alterDatabase(dbDefinition: CatalogDatabase): Unit
  //获取数据名字
  def getDatabase(db: String): CatalogDatabase
  //数据是否存在
  def databaseExists(db: String): Boolean
  //列出所有数据库
  def listDatabases(): Seq[String]
  //根据指定规则pattern列出所有数据库
  def listDatabases(pattern: String): Seq[String]
  //设置当前数据库
  def setCurrentDatabase(db: String): Unit

  // --------------------------------------------------------------------------
  // 数据表 层面
  // --------------------------------------------------------------------------

  //创建一张表，如果创建成功就返回tru，否则返回false
  def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit
  //删除数据表，入参是db[数据库],table[表],ignorenIfNostExists[是否有if not exists判断],pure[没咋看懂~~]
  def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit
  //重命名数据表，入参是db[数据库],oldName[之前表名称],newName[新表名称]
  def renameTable(db: String, oldName: String, newName: String): Unit

  /**
   * Alter a table whose database and name match the ones specified in `tableDefinition`, assuming
   * the table exists. Note that, even though we can specify database in `tableDefinition`, it's
   * used to identify the table, not to alter the table's database, which is not allowed.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(tableDefinition: CatalogTable): Unit

  /**
   * 更改由提供的数据库和表名标识的表的数据结构。
   * 新的数据结构不应具有与现有分区列冲突的列名, and
   * 并且仍应包含所有现有数据列。
   *
   * @param db 数据库 
   * @param table 表
   * @param newDataSchema 新的表结构schema
   */
  def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit

  //更改表的统计信息。如果“stats”为“none”，则删除所有现有统计。
  def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit
  //获取单个表，入参db[数据库],table[表]
  def getTable(db: String, table: String): CatalogTable
  //获取一个以上表，入参db[数据库],tables[数据表集合]
  def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable]
  //判断表是否存在
  def tableExists(db: String, table: String): Boolean
  //列出指定数据库下所有表
  def listTables(db: String): Seq[String]
  //根据指定规则pattern列出指定数据库下的所有表
  def listTables(db: String, pattern: String): Seq[String]

  /**
   * 数据落入表中
   *
   * @param isSrcLocal Whether the source data is local, as defined by the "LOAD DATA LOCAL"
   *                   HiveQL command.
   */
  def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit

  /**
   * 数据落入分区
   *
   * @param isSrcLocal Whether the source data is local, as defined by the "LOAD DATA LOCAL"
   *                   HiveQL command.
   */
  def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit

  def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit

  // --------------------------------------------------------------------------
  // 分区 层面
  // --------------------------------------------------------------------------

  //创建一个分区
  def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit
  //删除一个分区
  def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit

  /**
   * 覆盖一个或多个现有表分区的规范（假设它们存在）。
   * 这假定“specs”的索引i对应于“newspecs”的索引i
   */
  def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit

  /**
   * 更改其规范与“parts”中指定的规范匹配的一个或多个表分区，
   * 假设存在的分区。
   *
   * 注意: 如果底层实现类不支持分，则为禁止状态
   */
  def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit
  //获取分区
  def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition

  /**
   * 返回指定的分区，如果不存在，则返回“无”。
   */
  def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition]

  /**
   * 列出所有分区
   *
   * 对于具有分区列p1、p2、p3的表，每个分区名的格式为
   * ` p1=v1/p2=v2/p3=v3`。每个分区列名和值都是转义的路径名，可以
   * 用“externalCatalogUtils.unescapathName”方法解码。
   *
   * 返回是一个string集合
   *
   * 可以选择提供部分分区规范来过滤返回的分区, 
   * listPartitions` 方法中明细一样
   *
   * @param db 数据库
   * @param table 表
   * @param partialSpec 分区
   */
  def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String]

  /**
   * 列出属于指定表的所有分区的元数据（假定该表存在）。
   *
   * 可以选择提供部分分区规范来过滤返回的分区。
   * 如果存在分区（A=1'，B=2'，（A=1'，B=3'）和（A=2'，B=4'），
   * 然后（a='1'）的部分规范将只返回前两个。
   *
   * @param db 数据库
   * @param table 表名称
   * @param partialSpec 分区
   */
  def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition]

  /**
   * 列出属于指定表的分区的元数据（假定该表存在）
   * 满足给定的分区修剪谓词表达式。【什么是谓词表达式？】
   *
   * @param db 数据库
   * @param table 表名称
   * @param predicates 分区修剪谓词
   * @param defaultTimeZoneId default timezone id to parse partition values of TimestampType
   */
  def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition]

  // --------------------------------------------------------------------------
  // 方法 层面
  // --------------------------------------------------------------------------

  //创建方法，入参db[数据库],funcDefinition[方法]
  def createFunction(db: String, funcDefinition: CatalogFunction): Unit
  //删除方法,入参db[数据库],funcName[方法名称]
  def dropFunction(db: String, funcName: String): Unit
  //修改一个方法,入参db[数据库],fuincDefinition[方法体]
  def alterFunction(db: String, funcDefinition: CatalogFunction): Unit
  //重命名方法,入参db[数据库],oldName[旧名称],newName[新名称]
  def renameFunction(db: String, oldName: String, newName: String): Unit
  //获取一个方法体，入参db[数据库],funcNamep[方法名称]
  def getFunction(db: String, funcName: String): CatalogFunction
  //判断方法是否存在，入参db[数据库],funcName[方法名称]
  def functionExists(db: String, funcName: String): Boolean
  //根据指定规则列出所有方法，入参db[数据库]，parttern[指定规则]
  def listFunctions(db: String, pattern: String): Seq[String]
}
