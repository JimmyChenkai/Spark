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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils


/**
 * 这个类比较独立，没有任何继承和实现算是顶级类
 * 线程安全管理类，全局临时视图提供用于管理它们的原子操作,
 * e.g. create, update, remove, etc.
 *
 * 备注，这临时表示区分大小写的，调用者需要自己独立处理大小写的格式化
 *
 * @param 数据库系统保留的虚拟数据库，保留所有全局临时视图。
 */
class GlobalTempViewManager(val database: String) {

  /** 视图定义列表，从视图名称映射到逻辑计划。 */
  @GuardedBy("this")
  private val viewDefinitions = new mutable.HashMap[String, LogicalPlan]

  /**
   * 返回与给定名称匹配的全局视图定义，如果未找到，则返回“无”
   * 入参 name[匹配名称字符串]，返回满足调逻辑计划
   */
  def get(name: String): Option[LogicalPlan] = synchronized {
    viewDefinitions.get(name)
  }

  /**
   * 创建全局临时视图，或者在视图已存在且“overrideifixists”为false时发出异常。
   * 入参 name[视图名称],viewDefinition[视图逻辑计划], overrideIfExists[是否存在覆盖]
   */
  def create(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    if (!overrideIfExists && viewDefinitions.contains(name)) {
      throw new TempTableAlreadyExistsException(name)
    }
    viewDefinitions.put(name, viewDefinition)
  }

  /**
   * 更新全局临时视图（如果存在），如果更新则返回true，否则返回false。
   * 入参 name[视图名称],viewDefinition[视图逻辑计划]
   */
  def update(
      name: String,
      viewDefinition: LogicalPlan): Boolean = synchronized {
    if (viewDefinitions.contains(name)) {
      viewDefinitions.put(name, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   * 删除全局临时视图（如果存在），如果删除则返回true，否则返回false。
   * 入参 name[视图名称]
   */
  def remove(name: String): Boolean = synchronized {
    viewDefinitions.remove(name).isDefined
  }

  /**
   * 如果源视图存在而目标视图不存在，则重命名全局临时视图, 否则
   * 如果源视图存在但目标视图已存在，则发出异常。 
   * 如果成功就返回ture，否则返回false
   */
  def rename(oldName: String, newName: String): Boolean = synchronized {
    if (viewDefinitions.contains(oldName)) {
      if (viewDefinitions.contains(newName)) {
        throw new AnalysisException(
          s"rename temporary view from '$oldName' to '$newName': destination view already exists")
      }

      val viewDefinition = viewDefinitions(oldName)
      viewDefinitions.remove(oldName)
      viewDefinitions.put(newName, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   *列出所有全局临时视图的名称。
   */
  def listViewNames(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(viewDefinitions.keys.toSeq, pattern)
  }

  /**
   * 清楚所有全局临时视图
   */
  def clear(): Unit = synchronized {
    viewDefinitions.clear()
  }
}
