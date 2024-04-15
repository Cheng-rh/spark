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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, CountDownLatch}
import javax.annotation.concurrent.GuardedBy

import scala.concurrent.Promise
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread pool.
 *                       If 0, will consider the available CPUs on the host.
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {
  // RpcEndpoint 名称与 MessageLoop 之间的映射关系
  private val endpoints: ConcurrentMap[String, MessageLoop] =
    new ConcurrentHashMap[String, MessageLoop]
  // 端点实例 RpcEndpoint 与端点引用 RpcEndpointRef 之间的映射关系
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  private val shutdownLatch = new CountDownLatch(1)
  private lazy val sharedLoop = new SharedMessageLoop(nettyEnv.conf, this, numUsableCores)

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   */
  // Dispatcher 停止时为 true，一旦 Dispatcher 停止，所有发出去的消息都会被立即退回
  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 端点的RPC地址（类似于邮编地址）
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 端点编号，便于客户端调用（类似于邮编）
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpoints.containsKey(name)) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }

      // This must be done before assigning RpcEndpoint to MessageLoop, as MessageLoop sets Inbox be
      // active when registering, and endpointRef must be put into endpointRefs before onStart is
      // called.
      // 建立端点和端点编号的映射关系
      endpointRefs.put(endpoint, endpointRef)

      var messageLoop: MessageLoop = null
      try {
        messageLoop = endpoint match {
          case e: IsolatedRpcEndpoint =>
            new DedicatedMessageLoop(name, e, this)
          case _ =>
            // 将端点注册到共享线程池中
            sharedLoop.register(name, endpoint)
            sharedLoop
        }
        //端点名称与消息循环处理器的映射关系
        endpoints.put(name, messageLoop)
      } catch {
        case NonFatal(e) =>
          endpointRefs.remove(endpoint)
          throw e
      }
    }
    //将端点编号返回
    endpointRef
  }

  // 根据 RpcEndpoint 获取其对应的 RpcEndpointRef
  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  // 根据 RpcEndpoint 删除 RpcEndpoint 与 RpcEndpointRef 的映射缓存
  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // 幂等的，多次执行的结果是一样的
  private def unregisterRpcEndpoint(name: String): Unit = {
    val loop = endpoints.remove(name)
    if (loop != null) {
      loop.unregister(name)
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  // 停止具体某个 RpcEndpoint
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  // 向当前进程中所有注册过的 RpcEndpoint 发送消息
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug(s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
  }

  /** Posts a message sent by a remote endpoint. */
  // 发布由 Remote RpcEndpoint 发送的消息
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. */
  // 发布由 Local RpcEndpoint 发送的消息
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. */
  // 发布单向消息，不需要回复
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      {
        // SPARK-31922: in local cluster mode, there's always a RpcEnvStoppedException when
        // stop is called due to some asynchronous message handling. We catch the exception
        // and log it at debug level to avoid verbose error message when user stop a local
        // cluster in spark shell.
        case re: RpcEnvStoppedException => logDebug(s"Message $message dropped. ${re.getMessage}")
        case e if SparkEnv.get.isStopped =>
          logWarning(s"Message $message dropped due to sparkEnv is stopped. ${e.getMessage}")
        case e => throw e
      })
  }

  /**
   * Posts a message to a specific endpoint.
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */

  /**
   * 向指定端点发送消息
   *
   * @param endpointName 端点名称
   * @param message 需要发送的消息
   * @param callbackIfStopped 如果端点已经停止，回调此函数
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 根据端点名称获取对应的消息循环
      val loop = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (loop == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 如果当前 Dispatcher 不是停止状态
        loop.post(endpointName, message)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    // 最后调用回调函数
    error.foreach(callbackIfStopped)
  }

  // 停止 Dispatcher
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    var stopSharedLoop = false
    // 对所有 RpcEndpoint 执行取消注册，并关闭 MessageLoop
    endpoints.asScala.foreach { case (name, loop) =>
      unregisterRpcEndpoint(name)
      if (!loop.isInstanceOf[SharedMessageLoop]) {
        loop.stop()
      } else {
        stopSharedLoop = true
      }
    }
    if (stopSharedLoop) {
      sharedLoop.stop()
    }
    shutdownLatch.countDown()
  }

  def awaitTermination(): Unit = {
    shutdownLatch.await()
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }
}
