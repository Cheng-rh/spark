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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {
  // RPC 执行 ask 的默认超时时间，默认为120s
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  // 当前 RpcEndpointRef 对应的 RpcEndpoint 地址，虽然 用了 RpcAddress 包装
  // 但本质上就是 spark://host:port
  def address: RpcAddress

  // 当前 RpcEndpointRef 对应的 RpcEndpoint 名称
  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  // 发送单向异步消息。即只负责发送，不关心对方是否收到，最多发送一次
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a
   * [[AbortableRpcFuture]] to receive the reply within the specified timeout.
   * The [[AbortableRpcFuture]] instance wraps [[Future]] with additional `abort` method.
   *
   * This method only sends the message once and never retries.
   */
  // 向对应的 RpcEndpoint.receiveAndReply 发送消息并期望在指定的超时时间内收到回复
  // 只发送一次，不会重试。精准一次。
  def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  // 向对应的 RpcEndpoint.receiveAndReply 发送消息并期望在指定的超时时间内收到回复
  // 只发送一次，不会重试。精准一次。
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  // 向相应的 RpcEndpoint.receiveAndReply 发送消息，并在指定的超时时间内获取其结果，如果失败则抛出异常。
  // 这是一个阻塞动作，可能会花费很多时间，所以不要在 RpcEndpoint 的消息循环中调用它。
  // 返回的 T 为 RpcEndpoint 回复的消息
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}

/**
 * A wrapper for [[Future]] but add abort method.
 * This is used in long run RPC and provide an approach to abort the RPC.
 */
private[spark]
class AbortableRpcFuture[T: ClassTag](val future: Future[T], onAbort: Throwable => Unit) {
  def abort(t: Throwable): Unit = onAbort(t)
}
