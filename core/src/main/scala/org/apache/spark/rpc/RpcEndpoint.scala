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

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {@code constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  // 当前 RpcEndpoint 所属的RpcEnv
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  // 当前 RpcEndpoint 对应的引用类 RpcEndpointRef
  // 调用 onStart 时，self 将生效。当调用 onStop 时，self 将变为 null
  // 因为在 onStart 之前，RpcEndpoint 还没有被注册，并且没有有效的 RpcEndpointRef。所以在调用 onStart 之前不要调用 self。
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
   * unmatched message, `SparkException` will be thrown and sent to `onError`.
   */
  // 接收并处理来自 RpcEndpointRef.send 和 RpcCallContext.reply 的消息
  // 处理之后不需要给客户端回复
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(s"$self does not implement 'receive'")
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   */
  // 接收并处理来自 RpcEndpointRef.ask 的消息，需要给客户端回复
  // 回复是通过 RpcCallContext 实现的
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(s"$self won't reply anything"))
  }

  /**
   * Invoked when any exception is thrown during handling messages.
   */
  // 处理消息发生异常时调用
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   */
  // 当客户端与当前节点建立连接之后调用
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   */
  // 当客户端与当前节点断开连接之后调用
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  // 当客户端节点与当前节点之间的网络连接发生错误时调用
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  // 在当前节点真正开始处理消息之前进行调用
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  // 在当前节点停止时调用，做一些收尾工作
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  // 停止当前 RpcEndpoint
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint

/**
 * An endpoint that uses a dedicated thread pool for delivering messages.
 */
private[spark] trait IsolatedRpcEndpoint extends RpcEndpoint {

  /**
   * How many threads to use for delivering messages.
   *
   * Note that requesting more than one thread means that the endpoint should be able to handle
   * messages arriving from many threads at once, and all the things that entails (including
   * messages being delivered to the endpoint out of order).
   */
  def threadCount(): Int

}

/**
 * An endpoint that uses a dedicated thread pool for delivering messages and
 * ensured to be thread-safe.
 */
private[spark] trait IsolatedThreadSafeRpcEndpoint extends IsolatedRpcEndpoint {

  /**
   * Limit the threadCount to 1 so that messages are ensured to be handled in a thread-safe way.
   */
  final def threadCount(): Int = 1

}
