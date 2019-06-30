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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ListenerBus

/**
 * 包装外部目录以提供侦听器事件。
 * 这个类主要继承了ExternalCatalog类和ListenerBus。
 * 注意这里一个是extends一个是with。那么两者区别是什么呢？
 * ps:
 * 1，with在子类定义完之后，先执行子类再执行父类，这时父类中的方法会自动接收覆盖后的属性
 * 2，在没有with的情况下，会先执行父类，等父类执行完成后在执行子类
 */
class ExternalCatalogWithListener(delegate: ExternalCatalog)
  extends ExternalCatalog
    with ListenerBus[ExternalCatalogEventListener, ExternalCatalogEvent] {
  import CatalogTypes.TablePartitionSpec

  //需要封装的ExternalCatalog类 如下所示 `delegate`
  def unwrapped: ExternalCatalog = delegate

  //Post事件监听入口
  override protected def doPostEvent(
      listener: ExternalCatalogEventListener,
      event: ExternalCatalogEvent): Unit = {
    listener.onEvent(event)
  }

  // --------------------------------------------------------------------------
  // 数据库 层面
  // --------------------------------------------------------------------------
 
  /**
  * 创建数据库封装监听方法，入参dbDefinition[数据库定义], CatalogDatabase[catalog数据库],ignoreIfExists[是否存在]
  * 首先获取到指定数据库名称db,然后调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中创建数据库方法
  **/    
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = dbDefinition.name
    postToAll(CreateDatabasePreEvent(db))
    delegate.createDatabase(dbDefinition, ignoreIfExists)
    postToAll(CreateDatabaseEvent(db))
  }
  /**
  * 删除数据库封装监听方法，入参db[数据库],ignoreIfExists[是否存在],cascade[是否缓存]
  * 首先调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中删除数据库方法
  **/    
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    postToAll(DropDatabasePreEvent(db))
    delegate.dropDatabase(db, ignoreIfNotExists, cascade)
    postToAll(DropDatabaseEvent(db))
  }
 /**
  * 修改数据库封装监听方法，入参dbDefinition[数据库定义], CatalogDatabase[catalog数据库]
  * 首先调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中修改数据库方法
  **/ 
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val db = dbDefinition.name
    postToAll(AlterDatabasePreEvent(db))
    delegate.alterDatabase(dbDefinition)
    postToAll(AlterDatabaseEvent(db))
  }
  //重写ExternalCatalog类获取数据库方法 【或者说是封装了吧】
  override def getDatabase(db: String): CatalogDatabase = {
    delegate.getDatabase(db)
  }
  //重写ExternalCatalog类判断数据库是否存在方法
  override def databaseExists(db: String): Boolean = {
    delegate.databaseExists(db)
  }
  //重写ExternalCatalog类列出所有边的方法
  override def listDatabases(): Seq[String] = {
    delegate.listDatabases()
  }
  //重写ExternalCatalog类根据指定规则列出表方法
  override def listDatabases(pattern: String): Seq[String] = {
    delegate.listDatabases(pattern)
  }
  //重写ExternalCatalog类设置当前数据库方法
  override def setCurrentDatabase(db: String): Unit = {
    delegate.setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // 表 层面
  // --------------------------------------------------------------------------

  /**
  * 创建表封装监听方法，入参dbDefinition[数据库定义], ignoreIfExists[是否存在]
  * 首先获取数据库名称和表名称，同时获取当前spark版本信息
  * 然后调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中创建表方法
  **/       
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    val tableDefinitionWithVersion =
      tableDefinition.copy(createVersion = org.apache.spark.SPARK_VERSION)
    postToAll(CreateTablePreEvent(db, name))
    delegate.createTable(tableDefinitionWithVersion, ignoreIfExists)
    postToAll(CreateTableEvent(db, name))
  }
  /**
  * 删除表封装监听方法，入参db[数据库], table[表],ignoreIfExists[是否存在],purge[没看懂？]
  * 调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中创建表方法
  **/       
  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    postToAll(DropTablePreEvent(db, table))
    delegate.dropTable(db, table, ignoreIfNotExists, purge)
    postToAll(DropTableEvent(db, table))
  }
  /**
  * 重命名表封装监听方法，入参db[数据库], oldName[旧表名称],newName[新表名称]
  * 调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中重命名表名称方法
  **/ 
  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameTablePreEvent(db, oldName, newName))
    delegate.renameTable(db, oldName, newName)
    postToAll(RenameTableEvent(db, oldName, newName))
  }
  /**
  * 修改表封装监听方法，入参tableDefinition[数据表定义]
  * 首先根据tableDefinition获取数据和表名称
  * 调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中修改表名称方法
  **/ 
  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    postToAll(AlterTablePreEvent(db, name, AlterTableKind.TABLE))
    delegate.alterTable(tableDefinition)
    postToAll(AlterTableEvent(db, name, AlterTableKind.TABLE))
  }
  /**
  * 修改表结构封装监听方法，入参db[数据库],table[表],newDataSchema[新的表结构]
  * 调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中修改修改表结构schema的方法
  **/ 
  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.DATASCHEMA))
    delegate.alterTableDataSchema(db, table, newDataSchema)
    postToAll(AlterTableEvent(db, table, AlterTableKind.DATASCHEMA))
  }
  /**
  * 修改表统计封装监听方法，入参db[数据库],table[表],stats[catalog的统计信息]
  * 调用父类ListenerBus监听方法postToAll
  * 然后调用封装后的delegate中修改修改表统计的方法
  **/ 
  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.STATS))
    delegate.alterTableStats(db, table, stats)
    postToAll(AlterTableEvent(db, table, AlterTableKind.STATS))
  }
  //重写ExternalCatalog类设置获取表方法
  override def getTable(db: String, table: String): CatalogTable = {
    delegate.getTable(db, table)
  }
  //重写ExternalCatalog类通过名称获取表列表方法
  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    delegate.getTablesByName(db, tables)
  }
  //重写ExternalCatalog类判断是否存在表方法
  override def tableExists(db: String, table: String): Boolean = {
    delegate.tableExists(db, table)
  }
  //重写ExternalCatalog类列出指定数据库所有表方法
  override def listTables(db: String): Seq[String] = {
    delegate.listTables(db)
  }
  //重写ExternalCatalog类根据指定规则获取指定数据库中所有满足条件的表名称方法
  override def listTables(db: String, pattern: String): Seq[String] = {
    delegate.listTables(db, pattern)
  }
  /**
  * load数据进表封装监听方法，
  * 入参db[数据库],table[表],loadPath[需要load路径应该是HDFS路径],isOverwrite[是否覆盖],isSrcLocal[是否本地路径]
  * 封装后的delegate中load数据进表的方法
  **/ 
  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }
  /**
  * load数据进分区封装监听方法，
  * 入参db[数据库],table[表],loadPath[需要load路径应该是HDFS路径],partition[分区],
  * isOverwrite[是否覆盖],inheritTableSpecs[是否继承表规范],isSrcLocal[是否本地路径]
  * 封装后的delegate中load数据进分区的方法
  **/ 
  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadPartition(
      db, table, loadPath, partition, isOverwrite, inheritTableSpecs, isSrcLocal)
  }
  /**
  * load数据进动态分区封装监听方法，
  * 入参db[数据库],table[表],loadPath[需要load路径应该是HDFS路径],partition[分区],
  * replace[是否替换],numDP[没看懂什么鬼]
  * 封装后的delegate中load数据进动态分区的方法
  **/ 
  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = {
    delegate.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP)
  }

  // --------------------------------------------------------------------------
  // 分区 层面
  // --------------------------------------------------------------------------
  /**
  * 创建分区封装监听方法，
  * 入参db[数据库],table[表],parts[分区集合],ignoreIfExists[是否存在]
  * 封装后的delegate中创建分区的方法
  **/ 
  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    delegate.createPartitions(db, table, parts, ignoreIfExists)
  }
 /**
  * 删除分区封装监听方法，
  * 入参db[数据库],table[表],partSpecs[分区集合],ignoreIfExists[是否存在],purge[?],retainData[是否保留数据]
  * 封装后的delegate中删除分区的方法
  **/ 
  override def dropPartitions(
      db: String,
      table: String,
      partSpecs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    delegate.dropPartitions(db, table, partSpecs, ignoreIfNotExists, purge, retainData)
  }
  /**
  * 分区重命名封装监听方法，
  * 入参db[数据库],table[表],specs[旧分区集合],newSpecs[新分区集合]
  * 封装后的delegate中分区重命名的方法
  **/ 
  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    delegate.renamePartitions(db, table, specs, newSpecs)
  }
 /**
  * 分区修改封装监听方法，
  * 入参db[数据库],table[表],table[表],parts[分区集合]
  * 封装后的delegate中分区修改的方法
  **/ 
  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    delegate.alterPartitions(db, table, parts)
  }
  /** 重写ExternalCatalog类 获取分区方法**/ 
  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    delegate.getPartition(db, table, spec)
  }
  /** 重写ExternalCatalog类 获取分区方法**/ 
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    delegate.getPartitionOption(db, table, spec)
  }
  /** 重写ExternalCatalog类 列出指定数据库中标的所有分区名称方法**/ 
  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    delegate.listPartitionNames(db, table, partialSpec)
  }
 /** 重写ExternalCatalog类 列出指定数据库中标的所有分区具体value方法**/ 
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    delegate.listPartitions(db, table, partialSpec)
  }
 /** 重写ExternalCatalog类 根据特定过滤条件列出指定数据库中标的所有分区具体value方法**/ 
  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    delegate.listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)
  }

  // --------------------------------------------------------------------------
  // 方法层面
  // --------------------------------------------------------------------------
  /**
  * 创建方法封装监听方法，
  * 入参db[数据库],funcDefinition[方法定义]
  * 首先把创建方法监听事件封装经postToAll中
  * 封装后的delegate中创建方法的方法
  **/ 
  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    postToAll(CreateFunctionPreEvent(db, name))
    delegate.createFunction(db, funcDefinition)
    postToAll(CreateFunctionEvent(db, name))
  }
 /**
  * 删除方法封装监听方法，
  * 入参db[数据库],funcName[方法名称]
  * 首先把删除方法监听事件封装经postToAll中
  * 封装后的delegate中删除方法的方法
  **/ 
  override def dropFunction(db: String, funcName: String): Unit = {
    postToAll(DropFunctionPreEvent(db, funcName))
    delegate.dropFunction(db, funcName)
    postToAll(DropFunctionEvent(db, funcName))
  }
 /**
  * 删修改方法封装监听方法，
  * 入参db[数据库],funcDefinition[方法定义]
  * 首先把删除方法监听事件封装经postToAll中
  * 封装后的delegate中修改方法的方法
  **/ 
  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    postToAll(AlterFunctionPreEvent(db, name))
    delegate.alterFunction(db, funcDefinition)
    postToAll(AlterFunctionEvent(db, name))
  }
 /**
  * 重命名改方法封装监听方法，
  * 入参db[数据库],oldName[旧的方法名称],newName[新方法名称]
  * 首先把删除方法监听事件封装经postToAll中
  * 封装后的delegate中重命名方法的方法
  **/ 
  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameFunctionPreEvent(db, oldName, newName))
    delegate.renameFunction(db, oldName, newName)
    postToAll(RenameFunctionEvent(db, oldName, newName))
  }
 /** 重写ExternalCatalog类 获取方法 **/
  override def getFunction(db: String, funcName: String): CatalogFunction = {
    delegate.getFunction(db, funcName)
  }
/** 重写ExternalCatalog类 列出所有方法 **/
  override def functionExists(db: String, funcName: String): Boolean = {
    delegate.functionExists(db, funcName)
  }
/** 重写ExternalCatalog类 根据指定规则列出所有方法 **/
  override def listFunctions(db: String, pattern: String): Seq[String] = {
    delegate.listFunctions(db, pattern)
  }
}
