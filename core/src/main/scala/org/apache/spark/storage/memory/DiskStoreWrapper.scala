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

package org.apache.spark.storage

import java.io.{FileOutputStream, IOException}
import java.nio.ByteBuffer

import com.google.common.io.Closeables
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.io.ChunkedByteBuffer
import ucb.remotebuf._

trait DiskStoreLike {
  def getSize(blockId: BlockId): Long
  def put(blockId: BlockId)(writeFunc: java.io.OutputStream => Unit): Unit
  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit
  def getBytes(blockId: BlockId): ChunkedByteBuffer
  def remove(blockId: BlockId): Boolean
  def contains(blockId: BlockId): Boolean
}

/**
  * Remote Memory (disaggregation) Store
  *
  * Behaves as a DiskStore but tries to use remote memory instead of a disk. For now this assumes that RMEM is
  * inexhaustible, eventually we'll do something more clever.
  */
private[spark] class DiskStoreWrapper(
    logStats: Boolean,
    blockStore: DiskStoreLike
    ) extends Logging with DiskStoreLike {

  /* Various counters for performance monitoring (time in ns, sizes in bytes) */
  var totalStored: Long = 0 // Total number of bytes written to Rmem/Disk
  var timeStoring: Long = 0 // Total time spent writing to Rmem
  var curStored: Long = 0 // Total number of bytes stored on Rmem/Disk right now (used to calculate maxStored)
  var maxStored: Long = 0 // Maximum bytes stored on Rmem/Disk at any one time
  var totalRead: Long = 0 // Total number of bytes read back from Rmem
  var timeReading: Long = 0 // Total time spent reading from Rmem
  var timeOther: Long = 0 // Total time spent not reading/writing

  def getSize(blockId: BlockId): Long = {
    val startTime = System.nanoTime()

    val res = blockStore.getSize(blockId)

    val endTime = System.nanoTime()
    timeOther += endTime - startTime

    res
  }

  def put(blockId: BlockId)(writeFunc: java.io.OutputStream => Unit): Unit = {
    val startTime = if (logStats) System.nanoTime() else 0

    blockStore.put(blockId)(writeFunc)

    if (logStats) {
      val blockSize = getSize(blockId)
      totalStored += blockSize
      curStored += blockSize
      maxStored = if (curStored > maxStored) curStored else maxStored

      val endTime = System.nanoTime()
      timeStoring += endTime - startTime
    }
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    val startTime = if (logStats) System.nanoTime() else 0

    blockStore.putBytes(blockId, bytes)

    if (logStats) {
      val blockSize = bytes.size
      totalStored += blockSize
      curStored += blockSize
      maxStored = if (curStored > maxStored) curStored else maxStored

      val endTime = System.nanoTime()
      timeStoring += endTime - startTime
    }
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val startTime = if (logStats) System.nanoTime() else 0

    val bytes = blockStore.getBytes(blockId)

    if(logStats) {
      val endTime = System.nanoTime()

      totalRead += bytes.size
      timeReading += endTime - startTime
    }
    bytes
  }

  def remove(blockId: BlockId): Boolean = {
    val startTime = System.nanoTime()
    if (logStats) {
      val size = getSize(blockId)
      totalStored -= size
      curStored -= size
    }

    val res = blockStore.remove(blockId)

    val endTime = System.nanoTime()
    timeOther += endTime - startTime

    res
  }

  def contains(blockId: BlockId): Boolean = {
    val startTime = System.nanoTime()

    val res = blockStore.contains(blockId)

    val endTime = System.nanoTime()
    timeOther += endTime - startTime

    res
  }

  def report(): Unit = {
    val conv: Double = 1E-6
    if (logStats) {
      logInfo(s"(RmemStoreStats), totalWritten, $totalStored")
      logInfo(s"(RmemStoreStats), totalRead, $totalRead")
      logInfo(s"(RmemStoreStats), maximumSize, $maxStored")
      logInfo(s"(RmemStoreStats), timeWriting, " + timeStoring*conv)
      logInfo(s"(RmemStoreStats), timeReading, " + timeReading*conv)
      logInfo("(RmemStoreStats), timeOther, " + timeOther*conv)
    }
  }
}
