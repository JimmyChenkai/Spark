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

import java.util.UUID
import java.util.concurrent._

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * [[ BroadcastExchangeExec ]]收集，转换并最终广播结果
 * 改造后的SparkPlan。
 */
case class BroadcastExchangeExec(
    mode: BroadcastMode,
    child: SparkPlan) extends Exchange {

  private[sql] val runId: UUID = UUID.randomUUID

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    BroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  /**
   * 用于在`relationFuture`上注册回调。
   * 请注意，调用此字段不会开始执行广播作业。
   */
  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] = promise.future

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  @transient
  private[sql] lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
   // relationFuture用于“doExecute”。因此，我们可以在这里正确获取执行ID。
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val task = new Callable[broadcast.Broadcast[Any]]() {
      override def call(): broadcast.Broadcast[Any] = {
        //这将在另一个线程中运行。设置执行ID，以便我们可以连接这些作业正确执行       
        SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
          try {
            //在此处设置作业组，以便稍后可能会在必要时被groupId取消。
            sparkContext.setJobGroup(runId.toString, s"broadcast exchange (runId $runId)",
              interruptOnCancel = true)
            val beforeCollect = System.nanoTime()
            //使用executeCollect / executeCollectIterator来避免转换为Scala类型
            val (numRows, input) = child.executeCollectIterator()
            if (numRows >= 512000000) {
              throw new SparkException(
                s"Cannot broadcast the table with 512 million or more rows: $numRows rows")
            }

            val beforeBuild = System.nanoTime()
            longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)

            //构造关系。
            val relation = mode.transform(input, Some(numRows))

            val dataSize = relation match {
              case map: HashedRelation =>
                map.estimatedSize
              case arr: Array[InternalRow] =>
                arr.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
              case _ =>
                throw new SparkException("[BUG] BroadcastMode.transform returned unexpected " +
                  s"type: ${relation.getClass.getName}")
            }

            longMetric("dataSize") += dataSize
            if (dataSize >= (8L << 30)) {
              throw new SparkException(
                s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
            }

            val beforeBroadcast = System.nanoTime()
            longMetric("buildTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeBuild)

            //广播关系
            val broadcasted = sparkContext.broadcast(relation)
            longMetric("broadcastTime") += NANOSECONDS.toMillis(
              System.nanoTime() - beforeBroadcast)

            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.success(broadcasted)
            broadcasted
          } catch {
            // SPARK-24294：绕过scala bug：https：//github.com/scala/bug/issues/9554，我们抛出
            // SparkFatalException，它是Exception的子类。ThreadUtils.awaitResult
            //将捕获此异常并重新抛出包装的致命throwable。
            case oe: OutOfMemoryError =>
              val ex = new SparkFatalException(
                new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
                  "worker nodes. As a workaround, you can either disable broadcast by setting " +
                  s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                  s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
                  .initCause(oe.getCause))
              promise.failure(ex)
              throw ex
            case e if !NonFatal(e) =>
              val ex = new SparkFatalException(e)
              promise.failure(ex)
              throw ex
            case e: Throwable =>
              promise.failure(e)
              throw e
          }
        }
      }
    }
    BroadcastExchangeExec.executionContext.submit[broadcast.Broadcast[Any]](task)
  }

  override protected def doPrepare(): Unit = {
    //实现未来。
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }
}

object BroadcastExchangeExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange",
        SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
