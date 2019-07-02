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

import java.util.Locale

import org.apache.spark.sql.AnalysisException

/** A trait 表示函数所需资源类型的特征。 */
abstract class FunctionResourceType(val resourceType: String)

object JarResource extends FunctionResourceType("jar")

object FileResource extends FunctionResourceType("file")

// 我们不允许用户指定存档，因为是基于YARN资源调度的。
// 当加载资源时候，将抛出异常要求用户使用 --archive with spark submit.
object ArchiveResource extends FunctionResourceType("archive")

/**
* 判断是什么类型 参数
* jar,file,archive.除此之外就抛出异常不支持资源
**/
object FunctionResourceType {
  def fromString(resourceType: String): FunctionResourceType = {
    resourceType.toLowerCase(Locale.ROOT) match {
      case "jar" => JarResource
      case "file" => FileResource
      case "archive" => ArchiveResource
      case other =>
        throw new AnalysisException(s"Resource Type '$resourceType' is not supported.")
    }
  }
}

case class FunctionResource(resourceType: FunctionResourceType, uri: String)

/**
 * 表示类的简单trait ，该类可用于加载
 * 一个函数。因为只有一个sqlcontext可以加载资源，所以我们创建了这个特性
 * 避免显式传递sqlcontext。
 */
trait FunctionResourceLoader {
  def loadResource(resource: FunctionResource): Unit
}
//懒加载？
object DummyFunctionResourceLoader extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    throw new UnsupportedOperationException
  }
}
