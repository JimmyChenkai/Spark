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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * Exchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.
 */
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
}

/**
 *重复使用交换的包装器具有不同的输出，因为产生了两个交换
 *逻辑上相同的输出将具有不同的输出属性ID集，因此我们需要
 *保留原始ID，因为它们是下游运营商所期望的。
 */
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {

 //忽略规范化的这个包装器。
  override def doCanonicalize(): SparkPlan = child.canonicalized

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  // `ReusedExchangeExec`可以从它的子节点获得不同的输出属性id集合，我们需要
  //更新`outputPartitioning`和`outputOrdering`中的属性id。
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning match {
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }
}

/**
 *找出SparkPlan中的重复交换，然后使用相同的交换所有参考。
 */
case class ReuseExchange(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }
    //使用交换模式构建哈希映射以避免O（N * N）个sameResult调用。
    val exchanges = mutable.HashMap[StructType, ArrayBuffer[Exchange]]()
    plan.transformUp {
      case exchange: Exchange =>
         //具有相同结果的交换通常也具有相同的模式（相同的列名）。
        val sameSchema = exchanges.getOrElseUpdate(exchange.schema, ArrayBuffer[Exchange]())
        val samePlan = sameSchema.find { e =>
          exchange.sameResult(e)
        }
        if (samePlan.isDefined) {
          //保持此交换的输出，以下计划要求解决
          //属性。
          ReusedExchangeExec(exchange.output, samePlan.get)
        } else {
          sameSchema += exchange
          exchange
        }
    }
  }
}
