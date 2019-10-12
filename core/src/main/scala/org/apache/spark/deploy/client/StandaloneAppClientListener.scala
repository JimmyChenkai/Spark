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

package org.apache.spark.deploy.client

/**
 * 当各种事件发生时，能够及时通知StandaloneSchedulerBackend的回调
 * 目前有四个事件回调函数
 * 1.成功连接 2.断开连接  3.添加一个executor  4.移除一个executor
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait StandaloneAppClientListener {
  //  向master成功注册application，成功连接到集群
  def connected(appId: String): Unit

  //  断开连接
  //  断开连接可能是临时的，可能是原master故障，等待master切换后，会恢复连接
  def disconnected(): Unit

  //  application由于不可恢复的错误停止
  def dead(reason: String): Unit

  //  添加一个executor
  def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  //  移除一个executor
  def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit

  //  移除一个worker
  def workerRemoved(workerId: String, host: String, message: String): Unit
}
