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

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Ecvent事件一定是external catalog在有变动或者修改时候发生创建的。
 * 同时Event在变动前或者变动后任何一种情况下被释放（event应该记录下来）
 */
trait ExternalCatalogEvent extends SparkListenerEvent

/**
 * externalCatalog修改事件监听接口
 */
trait ExternalCatalogEventListener {
  def onEvent(event: ExternalCatalogEvent): Unit
}

/**
 * 当数据库 被创建或者删除时候  event被释放
 */
trait DatabaseEvent extends ExternalCatalogEvent {
  /**
   * 数据库
   */
  val database: String
}

/**
 * 数据库创建之前event被释放
 */
case class CreateDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * 数据库创建之后event被释放
 */
case class CreateDatabaseEvent(database: String) extends DatabaseEvent

/**
 * 数据库删除之前evnet被释放
 */
case class DropDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * 数据库成功删除之后evnet被释放
 */
case class DropDatabaseEvent(database: String) extends DatabaseEvent

/**
 * 数据库修改之前evnet被释放
 */
case class AlterDatabasePreEvent(database: String) extends DatabaseEvent

/**
 * 数据修改之后event被释放
 */
case class AlterDatabaseEvent(database: String) extends DatabaseEvent

/**
 * 当一张表被创建，删除，重命名时候evnet被释放
 */
trait TableEvent extends DatabaseEvent {
  /**
   * 数据表名称
   */
  val name: String
}

/**
 * 数据表被创建之前event时间被释放
 */
case class CreateTablePreEvent(database: String, name: String) extends TableEvent

/**
 * 数据表被创建之后event时间被释放
 */
case class CreateTableEvent(database: String, name: String) extends TableEvent

/**
 * 数据表被删除之前event时间被释放
 */
case class DropTablePreEvent(database: String, name: String) extends TableEvent

/**
 * 数据表被删除之后event时间被释放
 */
case class DropTableEvent(database: String, name: String) extends TableEvent

/**
 * 数据表被重命名之前event时间被释放
 */
case class RenameTablePreEvent(
    database: String,
    name: String,
    newName: String)
  extends TableEvent

/**
 * 数据表被重命名之后event事件被释放
 */
case class RenameTableEvent(
    database: String,
    name: String,
    newName: String)
  extends TableEvent

/**
 * 用于表明表的那一部分被修改. 
 * 如果调用普通的altertable API，则类型通常为table。
 */
object AlterTableKind extends Enumeration {
  val TABLE = "table"
  val DATASCHEMA = "dataSchema"
  val STATS = "stats"
}

/**
 * 表被修改前event事件被释放
 */
case class AlterTablePreEvent(
    database: String,
    name: String,
    kind: String) extends TableEvent

/**
 * 表被修改后event事件被释放
 */
case class AlterTableEvent(
    database: String,
    name: String,
    kind: String) extends TableEvent

/**
 * 方法被创建，删除，修改时候event事件被释放
 */
trait FunctionEvent extends DatabaseEvent {
  /**
   * 方法
   */
  val name: String
}

/**
 * 方法被创建前event事件被释放
 */
case class CreateFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被创建后event事件被释放
 */
case class CreateFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被删除前event事件被释放
 */
case class DropFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被删除后event事件被释放
 */
case class DropFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被修改前event事件被释放
 */
case class AlterFunctionPreEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被修改后event事件被释放
 */
case class AlterFunctionEvent(database: String, name: String) extends FunctionEvent

/**
 * 方法被重命名前event事件被释放
 */
case class RenameFunctionPreEvent(
    database: String,
    name: String,
    newName: String)
  extends FunctionEvent

/**
 * 方法被重命名后event事件被释放
 */
case class RenameFunctionEvent(
    database: String,
    name: String,
    newName: String)
  extends FunctionEvent
