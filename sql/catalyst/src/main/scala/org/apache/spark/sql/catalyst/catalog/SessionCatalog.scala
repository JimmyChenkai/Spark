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
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

object SessionCatalog {
  val DEFAULT_DATABASE = "default"
}

/**
 * 类创建就会创建 ExternalCatalog，GlobalTempViewManager，FunctionRegistry，SQLConf
 * Configuration，ParserInterface，FunctionResourceLoader
 * Spark会话使用的internal Catalog
 * 
 * 这个内部目录充当底层元存储（例如hive元存储）的代理，
 * 它还管理它所属的Spark会话的临时视图和函数。
 *
 * 这个类一定是线程安全的
 */
class SessionCatalog(
    externalCatalogBuilder: () => ExternalCatalog,
    globalTempViewManagerBuilder: () => GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader) extends Logging {
  import SessionCatalog._
  import CatalogTypes.TablePartitionSpec

  // 测试用
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      conf: SQLConf) {
    this(
      () => externalCatalog,
      () => new GlobalTempViewManager("global_temp"),
      functionRegistry,
      conf,
      new Configuration(),
      new CatalystSqlParser(conf),
      DummyFunctionResourceLoader)
  }

  // 测试用
  def this(externalCatalog: ExternalCatalog) {
    this(
      externalCatalog,
      new SimpleFunctionRegistry,
      new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))
  }

  //延迟初始化 externalCatalog和globalTempViewManager类
  lazy val externalCatalog = externalCatalogBuilder()
  lazy val globalTempViewManager = globalTempViewManagerBuilder()

  /** 临时视图列表，从表名映射到其逻辑计划。 */
  @GuardedBy("this")
  protected val tempViews = new mutable.HashMap[String, LogicalPlan]

  // 备注: 我们在这里跟踪当前数据库，因为某些操作没有指定
  // 数据库（例如，删除表my_table）。在这种情况下，我们必须首先
  // 检查临时视图或函数是否存在，如果不存在，则操作
  // 当前数据库中对应的项。
  @GuardedBy("this")
  protected var currentDb: String = formatDatabaseName(DEFAULT_DATABASE)

  private val validNameFormat = "([\\w_]+)".r

  /**
   * 检查给定的名称是否符合HIVE（“[a-za-z_0-9]+”），
   * 也就是说，如果这个名称只包含字符、数字和u。
   *
   * 此方法旨在过滤只满足指定条件
   * org.apache.hadoop.hive.metastore.metastoreutils.validatename。
   */
  private def validateName(name: String): Unit = {
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw new AnalysisException(s"`$name` is not a valid name for tables/databases. " +
        "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  /**
   * 设置表名格式，考虑大小写敏感度。
   */
  protected[this] def formatTableName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * 设置数据库名称格式，考虑大小写敏感度。
   */
  protected[this] def formatDatabaseName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  private val tableRelationCache: Cache[QualifiedTableName, LogicalPlan] = {
    val cacheSize = conf.tableRelationCacheSize
    CacheBuilder.newBuilder().maximumSize(cacheSize).build[QualifiedTableName, LogicalPlan]()
  }

  /** 此方法提供了一种获取缓存计划的方法。*/
  def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan = {
    tableRelationCache.get(t, c)
  }

  /** 此方法提供了一种获取缓存计划（如果该键存在）的方法。 */
  def getCachedTable(key: QualifiedTableName): LogicalPlan = {
    tableRelationCache.getIfPresent(key)
  }

  /** 此方法提供了一种缓存计划的方法。 */
  def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit = {
    tableRelationCache.put(t, l)
  }

  /** 此方法提供了使缓存计划无效的方法。 */
  def invalidateCachedTable(key: QualifiedTableName): Unit = {
    tableRelationCache.invalidate(key)
  }

  /** 此方法提供一种使所有缓存计划无效的方法。 */
  def invalidateAllCachedTables(): Unit = {
    tableRelationCache.invalidateAll()
  }

  /**
   * 此方法用于在将此路径存储到基础外部目录之前使给定路径合格。
   * 因此，当路径不包含方案时，在更改默认文件系统之后，不会更改此路径。
   * 
   */
  private def makeQualifiedPath(path: URI): URI = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri
  }
  //判断数据库是否存在，如果不存在就抛出NoSuchDatabaseException异常
  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }
  //判断表是否存在，如果不存在，就抛出NoSuchTableException异常
  private def requireTableExists(name: TableIdentifier): Unit = {
    if (!tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }
  }
  //判断表是否不存在，如果存在，就抛出TableAlreadyExistsException异常 
  //这个和上面刚好是相反操作
  //我刚第一眼看到时候比较疑惑，为什么要这么做，上面和相面本质上是一个事情
  //后来一看返回异常就知道，其实就做了两件事，抛出两个异常
  //但是我还是会觉得这两个方法能合并的。来个分支即可。这点我记下~
  private def requireTableNotExists(name: TableIdentifier): Unit = {
    if (tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new TableAlreadyExistsException(db = db, table = name.table)
    }
  }

  // ----------------------------------------------------------------------------
  // 数据库 层面
  // ----------------------------------------------------------------------------
  // 所有方法都是会和底层catalog直接
  // ----------------------------------------------------------------------------

  /**
  * 创建数据库
  * 首先判断db是否和全局临时数据库重名，如果重名抛出异常
  * 然后判断是否合法名称validateName
  * 调用externalCatalog创建数据库
  **/
  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    if (dbName == globalTempViewManager.database) {
      throw new AnalysisException(
        s"${globalTempViewManager.database} is a system preserved database, " +
          "you cannot create a database with this name.")
    }
    validateName(dbName)
    val qualifiedPath = makeQualifiedPath(dbDefinition.locationUri)
    externalCatalog.createDatabase(
      dbDefinition.copy(name = dbName, locationUri = qualifiedPath),
      ignoreIfExists)
  }
  /**
  * 删除数据库
  * 首先判断db是否和全局临时数据库重名，如果重名抛出异常
  * 然后判断里面表示否是合法【遍历】
  * 调用externalCatalog删除数据库
  **/
  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == DEFAULT_DATABASE) {
      throw new AnalysisException(s"Can not drop default database")
    }
    if (cascade && databaseExists(dbName)) {
      listTables(dbName).foreach { t =>
        invalidateCachedTable(QualifiedTableName(dbName, t.table))
      }
    }
    externalCatalog.dropDatabase(dbName, ignoreIfNotExists, cascade)
  }
 /**
  * 修改数据库
  * 首先判断db是否存在
  * 调用externalCatalog修改数据库
  **/
  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    requireDbExists(dbName)
    externalCatalog.alterDatabase(dbDefinition.copy(name = dbName))
  }
 /**
  * 获取数据库元数据信息
  * 首先判断db是否存在
  * 调用externalCatalog获取数据库元数据信息
  **/
  def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    externalCatalog.getDatabase(dbName)
  }
  //判断数据是否存在
  def databaseExists(db: String): Boolean = {
    val dbName = formatDatabaseName(db)
    externalCatalog.databaseExists(dbName)
  }
  //列出所有数据集合
  def listDatabases(): Seq[String] = {
    externalCatalog.listDatabases()
  }
  //根据指定规则列出所有数据库集合
  def listDatabases(pattern: String): Seq[String] = {
    externalCatalog.listDatabases(pattern)
  }
  //根据指定规则列出所有数据库集合
  def getCurrentDatabase: String = synchronized { currentDb }
 /**
  * 设置当前数据库
  * 首先判断格式化数据库名称，例如大小写敏感之类的
  * 然后判断数据库名称是否和全局临时数据库名称同名，如果同名就抛出异常
  * 然后设置当前数据库
  **/
  def setCurrentDatabase(db: String): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == globalTempViewManager.database) {
      throw new AnalysisException(
        s"${globalTempViewManager.database} is a system preserved database, " +
          "you cannot use it as current database. To access global temporary views, you should " +
          "use qualified name with the GLOBAL_TEMP_DATABASE, e.g. SELECT * FROM " +
          s"${globalTempViewManager.database}.viewName.")
    }
    requireDbExists(dbName)
    synchronized { currentDb = dbName }
  }

  /**
   * 获取未提供数据库位置时创建非默认数据库的路径
   * 通过用户
   */
  def getDefaultDBPath(db: String): URI = {
    val database = formatDatabaseName(db)
    new Path(new Path(conf.warehousePath), database + ".db").toUri
  }

  // ----------------------------------------------------------------------------
  // 表 层面
  // ----------------------------------------------------------------------------
  // 有两种表：临时视图和元存储表
  // 临时视图跨会话隔离，不属于任何特定的数据库。
  // 元存储表可以跨多个会话使用，因为它们的元数据保存在基础目录中。
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // | 仅与元存储表交互的方法|
  // ----------------------------------------------------

  /**
   * 在“tabledefinition”中指定的数据库中创建元存储表。
   * 如果未指定此类数据库，请在当前数据库中创建它。
   */
  def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    //判断数据表是否合法
    validateName(table)
    //新表定义
    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // 使表的Location是合理的
      val qualifiedTableLocation =
        makeQualifiedPath(tableDefinition.storage.locationUri.get)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    requireDbExists(db)
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        //如果表存在，又没有加if not exists就抛出异常
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else if (validateLocation) {
      validateTableLocation(newTableDefinition)
    }
    externalCatalog.createTable(newTableDefinition, ignoreIfExists)
  }
  //校验数据表的Location是否合法方法
  def validateTableLocation(table: CatalogTable): Unit = {
    // SPARK-19724: the default location of a managed table should be non-existent or empty.
    if (table.tableType == CatalogTableType.MANAGED &&
      !conf.allowCreatingManagedTableUsingNonemptyLocation) {
      val tableLocation =
        new Path(table.storage.locationUri.getOrElse(defaultTablePath(table.identifier)))
      val fs = tableLocation.getFileSystem(hadoopConf)

      if (fs.exists(tableLocation) && fs.listStatus(tableLocation).nonEmpty) {
        throw new AnalysisException(s"Can not create the managed table('${table.identifier}')" +
          s". The associated location('${tableLocation.toString}') already exists.")
      }
    }
  }

  /**
   * 更改由“tabledefinition”标识的现有元存储表的元数据。
   *
   * 
   * 如果“tabledefinition”中未指定数据库，则假定该表位于当前数据库中。
   *
   * 备注: 如果底层实现不支持更改某个字段，
   * 则这将成为一个禁止操作。
   */
  def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    val newTableDefinition = tableDefinition.copy(identifier = tableIdentifier)
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    externalCatalog.alterTable(newTableDefinition)
  }

  /**
   * 更改由提供的表标识符标识的表的数据结构. 
   * 新数据架构不应与现有分区列有冲突的列名, 
   * 并且应该仍然包含所有现有的数据列。
   *
   * @param identifier TableIdentifier
   * @param newdataschema更新了要用于表的数据结构
   */
  def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)

    val catalogTable = externalCatalog.getTable(db, table)
    val oldDataSchema = catalogTable.dataSchema
    // 不支持删除列操作
    // 否则抛出异常
    val nonExistentColumnNames =
      oldDataSchema.map(_.name).filterNot(columnNameResolved(newDataSchema, _))
    if (nonExistentColumnNames.nonEmpty) {
      throw new AnalysisException(
        s"""
           |Some existing schema fields (${nonExistentColumnNames.mkString("[", ",", "]")}) are
           |not present in the new schema. We don't support dropping columns yet.
         """.stripMargin)
    }

    externalCatalog.alterTableDataSchema(db, table, newDataSchema)
  }

  private def columnNameResolved(schema: StructType, colName: String): Boolean = {
    schema.fields.map(_.name).exists(conf.resolver(_, colName))
  }

  /**
   * 更改由提供的表标识符标识的现有元存储表的Spark统计信息
   * 
   */
  def alterTableStats(identifier: TableIdentifier, newStats: Option[CatalogStatistics]): Unit = {
    val db = formatDatabaseName(identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    externalCatalog.alterTableStats(db, table, newStats)
    // 使表关系缓存无效
    refreshTable(identifier)
  }

  /**
   * 返回具有指定名称的表/视图是否存在。如果未指定数据库, 
   * 检查当前数据库。
   * 底层是直接调用了catalog防范tableExists
   */
  def tableExists(name: TableIdentifier): Boolean = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    externalCatalog.tableExists(db, table)
  }

  /**
   * 检索现有永久表/视图的元数据. 如果未指定数据库,
   * 假设表/视图在当前数据库中。
   * 同时抛出两个异常 [NoSuchDatabaseException] [NoSuchTableException]
   */
  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.getTable(db, table)
  }

  /**
   * 检索现有永久表/视图的所有元数据。如果未指定数据库，
   * 假设表/视图在当前数据库中。
   * 只有属于同一数据库的表/视图才能被返回。
   * 例如，如果无法检索到任何请求的表，则返回一个空列表。
   * 不能保证指定的返回的表。
   * 同时抛出异常[NoSuchDatabaseException]
   */
  @throws[NoSuchDatabaseException]
  def getTablesByName(names: Seq[TableIdentifier]): Seq[CatalogTable] = {
    if (names.nonEmpty) {
      val dbs = names.map(_.database.getOrElse(getCurrentDatabase))
      if (dbs.distinct.size != 1) {
        val tables = names.map(name => formatTableName(name.table))
        val qualifiedTableNames = dbs.zip(tables).map { case (d, t) => QualifiedTableName(d, t)}
        throw new AnalysisException(
          s"Only the tables/views belong to the same database can be retrieved. Querying " +
          s"tables/views are $qualifiedTableNames"
        )
      }
      val db = formatDatabaseName(dbs.head)
      requireDbExists(db)
      val tables = names.map(name => formatTableName(name.table))
      externalCatalog.getTablesByName(db, tables)
    } else {
      Seq.empty
    }
  }

  /**
   * 将存储在给定路径中的文件加载到现有元存储表中。
   * 如果未指定数据库，则假定该表在当前数据库中。
   * 如果在数据库中找不到指定的表，则会引发[[NoSuchTableException]]。
   */
  def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    externalCatalog.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }

  /**
   * 将存储在给定路径中的文件加载到现有元存储表的分区中。
   * 如果未指定数据库，则假定该表在当前数据库中。
   * 如果在数据库中找不到指定的表，则会引发[[NoSuchTableException]]。
   */
  def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      spec: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.loadPartition(
      db, table, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)
  }
 //数据表默认路劲获取
  def defaultTablePath(tableIdent: TableIdentifier): URI = {
    val dbName = formatDatabaseName(tableIdent.database.getOrElse(getCurrentDatabase))
    val dbLocation = getDatabaseMetadata(dbName).locationUri

    new Path(new Path(dbLocation), formatTableName(tableIdent.table)).toUri
  }

  // ----------------------------------------------
  // | 仅与临时视图交互的方法 |
  // ----------------------------------------------

  /**
   * 创建一个本地临时视图
   * 如果临时视图存在并且没有加if not exists判断 则抛出异常
   */
  def createTempView(
      name: String,
      tableDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempViews.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempViews.put(table, tableDefinition)
  }

  /**
   * 创建一个全局临时视图
   */
  def createGlobalTempView(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = {
    globalTempViewManager.create(formatTableName(name), viewDefinition, overrideIfExists)
  }

  /**
   * 更改与给定名称匹配的本地/全局临时视图的定义, 
   * 如果临时视图匹配和更改，则返回true，否则返回false。
   */
  def alterTempViewDefinition(
      name: TableIdentifier,
      viewDefinition: LogicalPlan): Boolean = synchronized {
    val viewName = formatTableName(name.table)
    if (name.database.isEmpty) {
      if (tempViews.contains(viewName)) {
        createTempView(viewName, viewDefinition, overrideIfExists = true)
        true
      } else {
        false
      }
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.update(viewName, viewDefinition)
    } else {
      false
    }
  }

  /**
   *返回与存储时完全相同的本地临时视图。
   */
  def getTempView(name: String): Option[LogicalPlan] = synchronized {
    tempViews.get(formatTableName(name))
  }

  /**
   * 返回与存储时完全相同的全局临时视图。
   */
  def getGlobalTempView(name: String): Option[LogicalPlan] = {
    globalTempViewManager.get(formatTableName(name))
  }

  /**
   * 删除一个本地临时视图
   *
   * 如果成功删除此视图，则返回true，否则返回false。
   */
  def dropTempView(name: String): Boolean = synchronized {
    tempViews.remove(formatTableName(name)).isDefined
  }

  /**
   * 删除一个全局临时视图
   *
   * 如果成功删除此视图，则返回true，否则返回false。
   */
  def dropGlobalTempView(name: String): Boolean = {
    globalTempViewManager.remove(formatTableName(name))
  }

  // -------------------------------------------------------------
  // |与临时表和元存储表交互的方法 |
  // -------------------------------------------------------------

  /**
   * 检索现有临时视图或永久表/视图的元数据。
   *
   * 如果在“name”中指定了数据库，那么将返回表/视图的元数据。
   * 如果未指定数据库，则将首先尝试获取临时视图的元数据。
   * 如果不存在相同的名称，则返回表/视图的元数据。
   * 当前数据库。
   */
  def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable = synchronized {
    val table = formatTableName(name.table)
    if (name.database.isEmpty) {
      getTempView(table).map { plan =>
        CatalogTable(
          identifier = TableIdentifier(table),
          tableType = CatalogTableType.VIEW,
          storage = CatalogStorageFormat.empty,
          schema = plan.output.toStructType)
      }.getOrElse(getTableMetadata(name))
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(table).map { plan =>
        CatalogTable(
          identifier = TableIdentifier(table, Some(globalTempViewManager.database)),
          tableType = CatalogTableType.VIEW,
          storage = CatalogStorageFormat.empty,
          schema = plan.output.toStructType)
      }.getOrElse(throw new NoSuchTableException(globalTempViewManager.database, table))
    } else {
      getTableMetadata(name)
    }
  }

  /**
   * 表重命名
   *
   * 如果在“oldname”中指定了数据库，则将重命名该数据库中的表。
   * 如果未指定数据库，则将首先尝试使用
   * 如果不存在相同的名称，则重命名当前数据库中的表。
   *
   * 这假定在“newname”中指定的数据库与“oldname”中指定的数据库匹配。
   */
  def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = synchronized {
    val db = formatDatabaseName(oldName.database.getOrElse(currentDb))
    newName.database.map(formatDatabaseName).foreach { newDb =>
      if (db != newDb) {
        throw new AnalysisException(
          s"RENAME TABLE source and destination databases do not match: '$db' != '$newDb'")
      }
    }

    val oldTableName = formatTableName(oldName.table)
    val newTableName = formatTableName(newName.table)
    if (db == globalTempViewManager.database) {
      globalTempViewManager.rename(oldTableName, newTableName)
    } else {
      requireDbExists(db)
      if (oldName.database.isDefined || !tempViews.contains(oldTableName)) {
        requireTableExists(TableIdentifier(oldTableName, Some(db)))
        requireTableNotExists(TableIdentifier(newTableName, Some(db)))
        validateName(newTableName)
        validateNewLocationOfRename(oldName, newName)
        externalCatalog.renameTable(db, oldTableName, newTableName)
      } else {
        if (newName.database.isDefined) {
          throw new AnalysisException(
            s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': cannot specify database " +
              s"name '${newName.database.get}' in the destination table")
        }
        if (tempViews.contains(newTableName)) {
          throw new AnalysisException(s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': " +
            "destination table already exists")
        }
        val table = tempViews(oldTableName)
        tempViews.remove(oldTableName)
        tempViews.put(newTableName, table)
      }
    }
  }

  /**
   * 删除一张表
   *
   * 如果在“name”中指定了数据库，则将从该数据库中删除该表。
   * 如果未指定数据库，则将首先尝试删除具有
   * 如果不存在相同的名称，则从当前数据库中删除该表。
   */
  def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    if (db == globalTempViewManager.database) {
      val viewExists = globalTempViewManager.remove(table)
      if (!viewExists && !ignoreIfNotExists) {
        throw new NoSuchTableException(globalTempViewManager.database, table)
      }
    } else {
      if (name.database.isDefined || !tempViews.contains(table)) {
        requireDbExists(db)
        // When ignoreIfNotExists is false, no exception is issued when the table does not exist.
        // Instead, log it as an error message.
        if (tableExists(TableIdentifier(table, Option(db)))) {
          externalCatalog.dropTable(db, table, ignoreIfNotExists = true, purge = purge)
        } else if (!ignoreIfNotExists) {
          throw new NoSuchTableException(db = db, table = table)
        }
      } else {
        tempViews.remove(table)
      }
    }
  }

  /**
   * 返回代表给定表或视图的 [[LogicalPlan]]。
   *
   * 如果在“name”中指定了数据库，则将从该数据库返回表/视图。
   * 如果未指定数据库，则将首先尝试返回具有
   * 如果不存在相同的名称，则从当前数据库返回表/视图。
   *
   * 注意，全局临时视图数据库在这里也有效，这将返回全局临时
   * 与给定名称匹配的视图。
   *
   * 如果关系是一个视图，我们从视图描述生成一个[[View]]操作符，并且
   * 将逻辑计划包装在一个[[SubqueryAlias]]中，它将跟踪视图的名称。
   * [SubqueryAlias]]还将跟踪表/视图的名称和数据库（可选）
   *
   * @param name 
   */
  def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, db, viewDef)
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempViews.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        if (metadata.tableType == CatalogTableType.VIEW) {
          val viewText = metadata.viewText.getOrElse(sys.error("Invalid view without text."))
          logDebug(s"'$viewText' will be used for the view($table).")
          // 关系是一个视图，因此我们将关系包装为：
          // 1。在关系上添加一个[[View]]操作符以跟踪视图描述；
          // 2。将逻辑计划包装在跟踪视图名称的[[SubqueryAlias]]中。
          val child = View(
            desc = metadata,
            output = metadata.schema.toAttributes,
            child = parser.parsePlan(viewText))
          SubqueryAlias(table, db, child)
        } else {
          SubqueryAlias(table, db, UnresolvedCatalogRelation(metadata))
        }
      } else {
        SubqueryAlias(table, tempViews(table))
      }
    }
  }

  /**
   * 返回具有指定名称的表是否为临时视图。
   *
   * 注意：只有当数据库不在
   * 明确规定。
   */
  def isTemporaryTable(name: TableIdentifier): Boolean = synchronized {
    val table = formatTableName(name.table)
    if (name.database.isEmpty) {
      tempViews.contains(table)
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(table).isDefined
    } else {
      false
    }
  }

  /**
   * 列出指定数据库中的所有表，包括本地临时视图。
   *
   * 注意，如果指定的数据库是全局临时视图数据库，我们将列出全局
   * 临时视图。
   */
  def listTables(db: String): Seq[TableIdentifier] = listTables(db, "*")

  /**
   * 列出指定数据库中的所有匹配表，包括本地临时视图。
   *
   * 注意，如果指定的数据库是全局临时视图数据库，我们将列出全局
   * 临时视图。
   */
  def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbTables = if (dbName == globalTempViewManager.database) {
      globalTempViewManager.listViewNames(pattern).map { name =>
        TableIdentifier(name, Some(globalTempViewManager.database))
      }
    } else {
      requireDbExists(dbName)
      externalCatalog.listTables(dbName, pattern).map { name =>
        TableIdentifier(name, Some(dbName))
      }
    }
    val localTempViews = synchronized {
      StringUtils.filterPattern(tempViews.keys.toSeq, pattern).map { name =>
        TableIdentifier(name)
      }
    }
    dbTables ++ localTempViews
  }

  /**
   * 刷新元存储表的缓存项（如果有）。
   */
  def refreshTable(name: TableIdentifier): Unit = synchronized {
    val dbName = formatDatabaseName(name.database.getOrElse(currentDb))
    val tableName = formatTableName(name.table)

    //遍历临时视图并使其无效。
    //如果定义了数据库，则可能是全局临时视图。
    //如果没有定义数据库，很可能是临时视图。
    if (name.database.isEmpty) {
      tempViews.get(tableName).foreach(_.refresh())
    } else if (dbName == globalTempViewManager.database) {
      globalTempViewManager.get(tableName).foreach(_.refresh())
    }

    //同时使表关系缓存无效。
    val qualifiedTableName = QualifiedTableName(dbName, tableName)
    tableRelationCache.invalidate(qualifiedTableName)
  }

  /**
   * 删除所有现有临时视图。
   * 仅测试用
   */
  def clearTempTables(): Unit = synchronized {
    tempViews.clear()
  }

  // ----------------------------------------------------------------------------
  // 分区 层面
  // ----------------------------------------------------------------------------
  // 此类别中的所有方法都直接与基础目录交互。
  // 这些方法只涉及元存储表。
  // ----------------------------------------------------------------------------

  // TODO:我们需要弄清楚这些方法如何与数据源交互
  // 表。对于这样的表，我们不将分区列的值存储在
  // 元存储。目前，数据源表的分区值将是
  // 加载表时自动发现。

  /**
   * 在现有表中创建分区（假设它存在）。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.createPartitions(db, table, parts, ignoreIfExists)
  }

 /**
  * 从表中删除分区（假设它们存在）。
  * 如果未指定数据库，则假定该表在当前数据库中。
  */
  def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requirePartialMatchedPartitionSpec(specs, getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(specs)
    externalCatalog.dropPartitions(db, table, specs, ignoreIfNotExists, purge, retainData)
  }

 /**
  * 覆盖一个或多个现有表分区的规范（假设它们存在）。
  *
  * 这假定“specs”的索引i对应于“newspecs”的索引i。
  * 如果未指定数据库，则假定该表在当前数据库中。
  */
  def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val tableMetadata = getTableMetadata(tableName)
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(specs, tableMetadata)
    requireExactMatchedPartitionSpec(newSpecs, tableMetadata)
    requireNonEmptyValueInPartitionSpec(specs)
    requireNonEmptyValueInPartitionSpec(newSpecs)
    externalCatalog.renamePartitions(db, table, specs, newSpecs)
  }

  /**
   * 更改其规范与“parts”中指定的规范匹配的一个或多个表分区，
   * 假设分区存在。
   *
   * 如果未指定数据库，则假定该表在当前数据库中。
   *
   * 注意：如果底层实现不支持更改某个字段，
   * 这就成了禁忌。
   */
  def alterPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition]): Unit = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(parts.map(_.spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(parts.map(_.spec))
    externalCatalog.alterPartitions(db, table, parts)
  }

  /**
   * 检索表分区的元数据，假定它存在。
   * 如果未指定数据库，则假定该表在当前数据库中。
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    requireExactMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
    requireNonEmptyValueInPartitionSpec(Seq(spec))
    externalCatalog.getPartition(db, table, spec)
  }

  /**
   * 列出属于指定表的所有分区的名称（假定它存在）。
   *
   * 可以选择提供部分分区规范来过滤返回的分区。
   * 例如，如果存在分区（a='1'，b='2'）、（a='1'，b='3'）和（a='2'，b='4'），
   * 然后（a='1'）的部分规范将只返回前两个。
   */
  def listPartitionNames(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitionNames(db, table, partialSpec)
  }

  /**
   * 列出属于指定表的所有分区的元数据（假定该表存在）。
   *
   * 可以选择提供部分分区规范来过滤返回的分区。
   * 例如，如果存在分区（a='1'，b='2'）、（a='1'，b='3'）和（a='2'，b='4'），
   * 然后（a='1'）的部分规范将只返回前两个。
   */
  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    partialSpec.foreach { spec =>
      requirePartialMatchedPartitionSpec(Seq(spec), getTableMetadata(tableName))
      requireNonEmptyValueInPartitionSpec(Seq(spec))
    }
    externalCatalog.listPartitions(db, table, partialSpec)
  }

  /**
   * 列出属于指定表的分区的元数据，假设它存在，
   * 满足给定的分区修剪谓词表达式。
   */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    val db = formatDatabaseName(tableName.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableName.table)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Option(db)))
    externalCatalog.listPartitionsByFilter(db, table, predicates, conf.sessionLocalTimeZone)
  }

  /**
   * 验证输入分区规范是否有任何空值。
   */
  private def requireNonEmptyValueInPartitionSpec(specs: Seq[TablePartitionSpec]): Unit = {
    specs.foreach { s =>
      if (s.values.exists(_.isEmpty)) {
        val spec = s.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Partition spec is invalid. The spec ($spec) contains an empty partition column value")
      }
    }
  }

  /**
   * 验证输入分区规范是否与现有定义的分区规范完全匹配
   * 列必须相同，但顺序可能不同。
   */
  private def requireExactMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames.sorted
    specs.foreach { s =>
      if (s.keys.toSeq.sorted != defined) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${s.keys.mkString(", ")}) must match " +
            s"the partition spec (${table.partitionColumnNames.mkString(", ")}) defined in " +
            s"table '${table.identifier}'")
      }
    }
  }

  /**
   * 验证输入分区规范是否部分匹配现有定义的分区规范
   * 也就是说，分区规范的列应该是定义的分区规范的一部分。
   */
  private def requirePartialMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames
    specs.foreach { s =>
      if (!s.keys.forall(defined.contains)) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${s.keys.mkString(", ")}) must be contained " +
            s"within the partition spec (${table.partitionColumnNames.mkString(", ")}) defined " +
            s"in table '${table.identifier}'")
      }
    }
  }

  // ----------------------------------------------------------------------------
  // 方法层面
  // ----------------------------------------------------------------------------
  // 有两种函数：临时函数和元存储
  // 函数（永久UDF）。临时功能通过
  // 会话。元存储函数可以跨多个会话使用
  // 它们的元数据保存在基础目录中。
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // | 仅与元存储函数交互的方法 |
  // -------------------------------------------------------

  /**
   * 在“funcdefinition”中指定的数据库中创建函数。
   * 如果没有指定此类数据库，请在当前数据库中创建它。
   *
   * @param ignoreIfExists:是否有if not exists这样语法【sql语法吧】
   */
  def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (!functionExists(identifier)) {
      externalCatalog.createFunction(db, newFuncDefinition)
    } else if (!ignoreIfExists) {
      throw new FunctionAlreadyExistsException(db = db, func = identifier.toString)
    }
  }

  /**
   * 删除元存储函数。
   * 如果未指定数据库，则假定函数在当前数据库中。
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = name.copy(database = Some(db))
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
       // 如果我们已将此函数加载到函数注册表中，
       // 也从那里删掉。
       // 对于永久函数，因为我们将其加载到了函数注册表
       // 首次使用时，我们还需要将其从函数注册表中删除。
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.dropFunction(db, name.funcName)
    } else if (!ignoreIfNotExists) {
      throw new NoSuchFunctionException(db = db, func = identifier.toString)
    }
  }

  /**
   * 覆盖“funcdefinition”中指定的数据库中的元存储函数。
   * 如果未指定数据库，则假定函数在当前数据库中。
   */
  def alterFunction(funcDefinition: CatalogFunction): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (functionExists(identifier)) {
      if (functionRegistry.functionExists(identifier)) {
        // 如果我们已将此函数加载到函数注册表中，
        // 也从那里删除。
        // 对于永久函数，因为我们将其加载到了函数注册表
        // 首次使用时，我们还需要将其从函数注册表中删除。
        functionRegistry.dropFunction(identifier)
      }
      externalCatalog.alterFunction(db, newFuncDefinition)
    } else {
      throw new NoSuchFunctionException(db = db, func = identifier.toString)
    }
  }

  /**
   * 检索元存储函数的元数据。
   *
   * 如果在“name”中指定了数据库，则返回该数据库中的函数。
   * 如果未指定数据库，则返回当前数据库中的函数。
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    requireDbExists(db)
    externalCatalog.getFunction(db, name.funcName)
  }

  /**
   * 检查具有指定名称的函数是否存在
   */
  def functionExists(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name) || {
      val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
      requireDbExists(db)
      externalCatalog.functionExists(db, name.funcName)
    }
  }

  // ----------------------------------------------------------------
  // | 与临时和元存储函数交互的方法 |
  // ----------------------------------------------------------------

  /**
   * 基于提供的表示函数的类构造一个[[FunctionBuilder]]。
   * 构造一个函数生成器
   */
  private def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder = {
    val clazz = Utils.classForName(functionClassName)
    (input: Seq[Expression]) => makeFunctionExpression(name, clazz, input)
  }

  /**
   * 基于提供的表示函数的类构造[[Expression]]。
   *
   * 这将执行反射以决定要在生成器中返回哪种类型的[[Expression]]。
   */
  protected def makeFunctionExpression(
      name: String,
      clazz: Class[_],
      input: Seq[Expression]): Expression = {
    // 不幸的我们需要使用反射来，因为userdefinedaggregatefunction
    // scalaudaf /定义在SQL /核心模块。
    val clsForUDAF =
      Utils.classForName("org.apache.spark.sql.expressions.UserDefinedAggregateFunction")
    if (clsForUDAF.isAssignableFrom(clazz)) {
      val cls = Utils.classForName("org.apache.spark.sql.execution.aggregate.ScalaUDAF")
      val e = cls.getConstructor(classOf[Seq[Expression]], clsForUDAF, classOf[Int], classOf[Int])
        .newInstance(input,
          clazz.getConstructor().newInstance().asInstanceOf[Object], Int.box(1), Int.box(1))
        .asInstanceOf[ImplicitCastInputTypes]

      // 检查输入长度
      if (e.inputTypes.size != input.size) {
        throw new AnalysisException(s"Invalid number of arguments for function $name. " +
          s"Expected: ${e.inputTypes.size}; Found: ${input.size}")
      }
      e
    } else {
      throw new AnalysisException(s"No handler for UDAF '${clazz.getCanonicalName}'. " +
        s"Use sparkSession.udf.register(...) instead.")
    }
  }

  /**
   * 为函数加载JAR和文件等资源。每个资源都有代表性
   * 通过元组（资源类型，资源URI）。
   */
  def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  /**
   * 将临时或永久函数注册到会话特定的[[FunctionRegistry]]
   */
  def registerFunction(
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      functionBuilder: Option[FunctionBuilder] = None): Unit = {
    val func = funcDefinition.identifier
    if (functionRegistry.functionExists(func) && !overrideIfExists) {
      throw new AnalysisException(s"Function $func already exists")
    }
    val info = new ExpressionInfo(funcDefinition.className, func.database.orNull, func.funcName)
    val builder =
      functionBuilder.getOrElse {
        val className = funcDefinition.className
        if (!Utils.classIsLoadable(className)) {
          throw new AnalysisException(s"Can not load class '$className' when registering " +
            s"the function '$func', please make sure it is on the classpath")
        }
        makeFunctionBuilder(func.unquotedString, className)
      }
    functionRegistry.registerFunction(func, info, builder)
  }

  /**
   * 删除临时函数。
   */
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!functionRegistry.dropFunction(FunctionIdentifier(name)) && !ignoreIfNotExists) {
      throw new NoSuchTempFunctionException(name)
    }
  }

  /**
   * 返回它是否为临时函数。如果不存在，则返回false。
   */
  def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
    // 从hivesessioncatalog复制
    val hiveFunctions = Seq("histogram_numeric")

    // 临时函数是在函数注册表中注册的函数
    // 没有数据库名称，既不是内置函数，也不是配置单元函数
    name.database.isEmpty &&
      functionRegistry.functionExists(name) &&
      !FunctionRegistry.builtin.functionExists(name) &&
      !hiveFunctions.contains(name.funcName.toLowerCase(Locale.ROOT))
  }

  /**
   * 返回此函数是否已在当前的函数注册表中注册
   * 如果不存在，返回false。
   */
  def isRegisteredFunction(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name)
  }

  /**
   * 返回它是否为持久函数。如果不存在，则返回false。
   */
  def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    databaseExists(db) && externalCatalog.functionExists(db, name.funcName)
  }

  protected[sql] def failFunctionLookup(
      name: FunctionIdentifier, cause: Option[Throwable] = None): Nothing = {
    throw new NoSuchFunctionException(
      db = name.database.getOrElse(getCurrentDatabase), func = name.funcName, cause)
  }

  /**
   * 查找与指定函数关联的[[expressioninfo]]，假定该函数存在。
   */
  def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    // TODO: 只需使函数注册表接受函数标识符，而不是复制它
    val database = name.database.orElse(Some(currentDb)).map(formatDatabaseName)
    val qualifiedName = name.copy(database = database)
    functionRegistry.lookupFunction(name)
      .orElse(functionRegistry.lookupFunction(qualifiedName))
      .getOrElse {
        val db = qualifiedName.database.get
        requireDbExists(db)
        if (externalCatalog.functionExists(db, name.funcName)) {
          val metadata = externalCatalog.getFunction(db, name.funcName)
          new ExpressionInfo(
            metadata.className,
            qualifiedName.database.orNull,
            qualifiedName.identifier)
        } else {
          failFunctionLookup(name)
        }
      }
  }

  /**
   * 返回代表指定函数的[[表达式]]，假定该函数存在。
   *
   * 对于已加载的临时函数或永久函数，
   * 此方法只需通过
   * 并基于生成器创建表达式。
   *
   * 对于尚未加载的永久函数，我们将首先获取其元数据
   * 来自基础外部目录。然后，我们将加载所有相关联的资源
   * 使用此功能（即JAR和文件）。最后，我们创建一个函数生成器
   * 基于函数类并将生成器放入函数注册表。
   * 函数注册表中此函数的名称将为“databasename.function name”。
   */
  def lookupFunction(
      name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    // 注意：这个函数的实现有点复杂。
    // 我们可能不应该使用单个函数注册表来注册所有三种函数（内置、临时和外部）。
    if (name.database.isEmpty && functionRegistry.functionExists(name)) {
      //此函数已加载到函数注册表中。
      return functionRegistry.lookupFunction(name, children)
    }

    //如果名称本身不合格，则添加当前数据库。
    val database = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val qualifiedName = name.copy(database = Some(database))

    if (functionRegistry.functionExists(qualifiedName)) {
     // 此函数已加载到函数注册表中。
     // 与上面的块不同，我们使用限定名来查找此函数。
      return functionRegistry.lookupFunction(qualifiedName, children)
    }

    // 函数尚未加载到函数注册表中，这意味着
    // 该函数是一个永久函数（如果它实际上已经注册
    // 在元存储中）。我们需要首先将函数放入函数注册表。
    // TODO: why not just check whether the function exists first?
    val catalogFunction = try {
      externalCatalog.getFunction(database, name.funcName)
    } catch {
      case _: AnalysisException => failFunctionLookup(name)
      case _: NoSuchPermanentFunctionException => failFunctionLookup(name)
    }
    loadFunctionResources(catalogFunction.resources)
    // 请注意，QualifiedName是由用户提供的。然而，
    // CatalogFunction.Identifier.UnQuotedString由底层返回
    // 目录。因此，有可能QualifiedName与
    // catalogFunction.identifier.unquotedString（区别在于区分大小写）。
    // 在这里，我们保留来自用户的输入。
    registerFunction(catalogFunction.copy(identifier = qualifiedName), overrideIfExists = false)
    // 开始创建Expression
    functionRegistry.lookupFunction(qualifiedName, children)
  }

  /**
   * 列出指定数据库中的所有函数，包括临时函数。这个
   * 返回函数标识符及其定义范围（系统或用户
   * 已定义）。
   */
  def listFunctions(db: String): Seq[(FunctionIdentifier, String)] = listFunctions(db, "*")

  /**
   * 列出指定数据库中的所有匹配函数，包括临时函数。这个
   * 返回函数标识符及其定义范围（系统或用户
   * 已定义）。
   */
  def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    val dbFunctions = externalCatalog.listFunctions(dbName, pattern).map { f =>
      FunctionIdentifier(f, Some(dbName)) }
    val loadedFunctions = StringUtils
      .filterPattern(functionRegistry.listFunction().map(_.unquotedString), pattern).map { f =>
       // 在functionregistry中，函数名存储为不带引号的格式。
        Try(parser.parseFunctionIdentifier(f)) match {
          case Success(e) => e
          case Failure(_) =>
            //某些内置函数的名称不能由分析器分析，例如%
            FunctionIdentifier(f)
        }
      }
    val functions = dbFunctions ++ loadedFunctions
    // 会话目录缓存函数注册表中的一些持久函数
    // 所以可能有重复。
    functions.map {
      case f if FunctionRegistry.functionSet.contains(f) => (f, "SYSTEM")
      case f => (f, "USER")
    }.distinct
  }


  // -----------------
  // | 其他方法 |
  // -----------------

  /**
   * 删除所有现有数据库（除“默认”）、表、分区和函数，
   * 并将当前数据库设置为“默认”。
   *
   * 主要用于测试。
   */
  def reset(): Unit = synchronized {
    setCurrentDatabase(DEFAULT_DATABASE)
    externalCatalog.setCurrentDatabase(DEFAULT_DATABASE)
    listDatabases().filter(_ != DEFAULT_DATABASE).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }
    listTables(DEFAULT_DATABASE).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
    }
    listFunctions(DEFAULT_DATABASE).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    clearTempTables()
    globalTempViewManager.clear()
    functionRegistry.clear()
    tableRelationCache.invalidateAll()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  /**
   * 将目录的当前状态复制到其他目录。
   *
   * 此函数在此[[sessioncatalog]]（源）上同步，以确保复制的
   * 状态是一致的。目标[[sessioncatalog]]未同步，不应
   * 因为此时不应发布目标[[sessioncatalog]]。呼叫方必须
   * 如果此假设不成立，则在目标上同步。
   */
  private[sql] def copyStateTo(target: SessionCatalog): Unit = synchronized {
    target.currentDb = currentDb
    //复制临时视图
    tempViews.foreach(kv => target.tempViews.put(kv._1, kv._2))
  }

  /**
   * 在重命名不存在的托管表之前，请验证新的location。
   */
  private def validateNewLocationOfRename(
      oldName: TableIdentifier,
      newName: TableIdentifier): Unit = {
    val oldTable = getTableMetadata(oldName)
    if (oldTable.tableType == CatalogTableType.MANAGED) {
      val databaseLocation =
        externalCatalog.getDatabase(oldName.database.getOrElse(currentDb)).locationUri
      val newTableLocation = new Path(new Path(databaseLocation), formatTableName(newName.table))
      val fs = newTableLocation.getFileSystem(hadoopConf)
      if (fs.exists(newTableLocation)) {
        throw new AnalysisException(s"Can not rename the managed table('$oldName')" +
          s". The associated location('$newTableLocation') already exists.")
      }
    }
  }
}
