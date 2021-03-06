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

/**
 * Remote Memory (disaggregation) Store
 *
 * Behaves as a DiskStore but tries to use remote memory instead of a disk. For now this assumes that RMEM is
 * inexhaustible, eventually we'll do something more clever.
 */
private[spark] class RmemStore(
    conf: SparkConf,
    diskManager: DiskBlockManager) extends Logging with DiskStoreLike {

  val RmemPort = conf.get("spark.executor.rmemPort", "12345")
  val RmemServer = try {
    conf.get("spark.executor.rmemServer")
  } catch {
    case e: NoSuchElementException =>
      throw new IllegalStateException(
        "Missing Rmem settings, please set " + e.getMessage + " in spark-defaults.conf")
  }
  logInfo("(RMEM) Connecting to Server: " + RmemServer + ":" + RmemPort)

  val BM = new RemoteBuf.BufferManager(RmemServer, RmemPort)

  def getSize(blockId: BlockId): Long = {
    logTrace(s"RMEM getSize($blockId)")

    try {
      return BM.getBuffer(blockId.name).getSize()
    } catch {
      /* Disk store would create an empty file and return 0 here. We are more strict */
      case ex: Throwable =>
        logWarning(s"Getting size of non-existent block $blockId")
        return 0L
    }
  }

  def put(blockId: BlockId)(writeFunc: java.io.OutputStream => Unit): Unit = {
    logTrace(s"RMEM put($blockId)")

    if (BM.bufferExists(blockId.name)) {
      logWarning(s"put($blockId) Trying to put pre-existing block")
      throw new IllegalStateException(s"Block $blockId is already present in the RMEM store")
    }

    val startTime = System.nanoTime

    val RBuf = BM.createBuffer(blockId.name)
    val RBufStream = new ROutputStream(RBuf)

    try {
      writeFunc(RBufStream)
    } catch {
      /* DiskStore would handle this gracefully, we fail hard */
      case ex: Throwable =>
        logError(s"Error writing block $blockId")
        throw ex
    } finally {
      RBufStream.close()
    }

    val finishTime = System.nanoTime
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    logTrace(s"RMEM putBytes($blockId)")

    /* This is nasty copy-pasta from put().
       I should really come up with a way to do this better... */
    if (BM.bufferExists(blockId.name)) {
      logWarning(s"putBytes($blockId) - trying to put pre-existing block")
      throw new IllegalStateException(s"Block $blockId is already present in the RMEM store")
    }

    val startTime = System.nanoTime

    val RBuf = BM.createBuffer(blockId.name)
    val RBufChan = new RWritableByteChannel(RBuf)

    try {
      bytes.writeFully(RBufChan)
    } catch {
      case ex: Throwable =>
        /* DiskStore would fail gracefully, we don't */
        logError(s"Error writing (putBytes) block $blockId")
        throw ex
    } finally {
      RBufChan.close()
    }

    val finishTime = System.nanoTime
    logDebug("Block %s stored to RMEM in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    logTrace(s"RMEM getBytes($blockId)")
    val RBuf = try {
      BM.getBuffer(blockId.name)
    } catch {
      /* Diskstore would create a new block and return 0 bytes, we fail hard */
      case ex: Throwable =>
        logError(s"Trying to get bytes from non-existent block $blockId")
        throw ex
    }

    logTrace("RMEM geting " + RBuf.getSize + s" bytes for $blockId")
    val localBuf = ByteBuffer.allocateDirect(RBuf.getSize())
    try {
      RBuf.read(localBuf)
      logTrace(s"RMEM getBytes($blockId) read " + localBuf.limit() + " bytes")
      new ChunkedByteBuffer(localBuf)
    } catch {
      case ex: Throwable =>
        logWarning(s"Failed to read buffer for block $blockId")
        throw new IOException("Failed while reading block " + blockId.name + " from RMEM")
    }
  }

  def remove(blockId: BlockId): Boolean = {
    logTrace(s"RMEM remove($blockId)")
    if (this.contains(blockId)) {
      try {
        BM.deleteBuffer(blockId.name)
        true
      } catch {
        case _: Throwable =>
          logWarning(s"Failed to delete buffer $blockId")
          false
      }
    } else {
      logWarning(s"Removing non-existent buffer $blockId")
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    logTrace(s"RMEM contains($blockId)")
    BM.bufferExists(blockId.name)
  }

  def reportStats(): String = {
    BM.reportStats();
  }
}
