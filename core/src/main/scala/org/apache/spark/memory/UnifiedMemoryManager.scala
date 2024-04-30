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

package org.apache.spark.memory

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
    // 尝试为当前内存获取最多的“numBytes”个执行内存，并返回获得的的字节数，如果不能分配则返回0
    // 在某些情况下，吃调用可能会被阻塞，直到有足够的可用内存为止，以确保每个任务`在总内存池被强制溢写之前`
    // 有机会增加到最少总内存池的1/2n（n是活动任务的数量）
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    // 根据存储模型，获取相应的Pool.
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.

        //Execution Pool中没有足够的可用内存，因此尝试从Storage中回收内存。
        //1.获取可从Storage取回的最大内存 = max (空余内存，借用内存)，这个借用内存是Storage从Execution借用的内存。
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          //2. 仅回收必要和可用的内存
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          //3.修改storagePool、executionPool可用空间
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
     */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    //2. 向ExecutionPool申请内存，申请前会执行以下步骤
    //maybeGrowExecutionPool: 通过驱逐缓冲的块扩大执行内存，从而缩小存储池
    //computeMaxExecutionPoolSize:计算清除存储内存后执行池的大小
    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize())
  }

  /**
   * 申请存储内存
   * @param blockId blockid
   * @param numBytes申请内存大小
   * @param memoryMode 内存模式
   *  @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)

    //1. 判断内存模式，从而获取对应额Pool，并计算出最大已经使用的内存
    // maxMemory = Heap总内存 - ExecutionMemoryPool中已经使用的内存
    // 之所以这样是因为，Execution的内存不能随便释放，而Storage的可以写入内存。
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    //2.1 如果总的空闲内存（Storage + Execution剩余）不够，则直接返回false
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    //2.2 如果总的空余内存够了，而Storage中的内存不足，那么久需要从ExecutionPool中借了
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    //2.3 如果StoragePool中的内存就够了，那么直接从中申请就可以了
    storagePool.acquireMemory(blockId, numBytes)
  }
//可以看到，实际上是直接申请StorageMemory。并且，由于可以动态占用，因此就不用像StaticMemoryManager那样去限制UnrollMemory的大小，
// 而是交给acquireStorageMemory()，只要有可用StorageMemory内存，便可以申请。
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    // 获取最大内存
    val maxMemory = getMaxMemory(conf)

    // 最大存储内存为：最大内存 * 0.5
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      // 存储内存为最大可用内存 * 0.5
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    //获取当前jvm最大内存
    val systemMemory = conf.get(TEST_MEMORY)
    //获取系统预留内存，默认为300M
    val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    //最小内存限制为：系统预留内存: 300M * 1.5
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_DRIVER_MEMORY",
        messageParameters = Map(
          "systemMemory" -> systemMemory.toString,
          "minSystemMemory" -> minSystemMemory.toString,
          "config" -> config.DRIVER_MEMORY.key))
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    // 校验Executor内存
    if (conf.contains(config.EXECUTOR_MEMORY)) {
      val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
      if (executorMemory < minSystemMemory) {
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_EXECUTOR_MEMORY",
          messageParameters = Map(
            "executorMemory" -> executorMemory.toString,
            "minSystemMemory" -> minSystemMemory.toString,
            "config" -> config.EXECUTOR_MEMORY.key))
      }
    }
    // 可使用内存 = 系统最大内存 - 系统预留内存
    val usableMemory = systemMemory - reservedMemory

    // 存储内存 + 计算内存 = 可使用内存 * 0.6
    // 例如如果我们有1GB的JVM内存，默认情况下执行和存储内存将为（1024 - 300）* 0.6 = 434 MB
    val memoryFraction = conf.get(config.MEMORY_FRACTION)
    (usableMemory * memoryFraction).toLong
  }
}
