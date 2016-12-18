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

package org.apache.spark.shuffle

import java.io._
import java.nio.file.{Files, Paths}

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

import ucb.remotebuf._

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
  private val BM1 = new RemoteBuf.BufferManager(1)
//  private val BM2 = new RemoteBuf.BufferManager(2)

  def getDataFile(shuffleId: Int, mapId: Int): File = {
//    val shufblockid = 
//    logTrace(s"getDataFile called with ${blockManager.diskBlockManager}")
//    logTrace(s"asking for ${shufblockid.name}")
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    if (index.length() != (blocks + 1) * 8) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   * */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {

    val bbuf = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(bbuf)

    var offset = 0L
    out2.writeLong(offset)
    for (length <- lengths) {
      offset += length
      out2.writeLong(offset)
    }
    out2.flush()
    out2.close()

//    val dataFile = getDataFile(shuffleId, mapId)
//    logTrace(s"writeIndexFileAndCommit using real data file: ${dataFile}")
//    logTrace(s"writeIndexFileAndCommit temp data file is: ${dataTmp}")

    val DataBaseName = "shuffle_" + shuffleId.toString() + "_" + mapId.toString() + ".data"
    val IndexBaseName = "shuffle_" + shuffleId.toString() + "_" + mapId.toString() + ".index"

    val BM = BM1 //if ((mapId % 2) == 0) BM1 else BM2

    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      // TODO: S: cheaper to just write again over RDMA vs reading to check each time?
//      logTrace(s"writeIndexFileAndCommit first successful map output write for ${dataFile}")

//      val t8 = System.nanoTime()
      val indexByteArray = bbuf.toByteArray()
//      val t9 = System.nanoTime()
//    logTrace(s"bbuf index conversion took ${(t9 - t8)/1000} us")

//      logTrace(s"RDMA sent index for ${IndexBaseName}")
//      val t0 = System.nanoTime()

      BM.write(IndexBaseName, indexByteArray, indexByteArray.length)
//      val t1 = System.nanoTime()

//      logTrace(s"RDMA sent index for ${IndexBaseName} complete")

      if (dataTmp != null && dataTmp.exists()) {
//        logTrace(s"RDMA sending data for ${DataBaseName}")
//        val t2 = System.nanoTime()

        BM.write_file(dataTmp.toString(), DataBaseName)
//        val t3 = System.nanoTime()

//        logTrace(s"RDMA sent data for ${DataBaseName} complete")
        dataTmp.delete()
//    logTrace(s"RDMA ${DataBaseName} write took ${(t3 - t2)/1000} us")


      }
//    logTrace(s"RDMA ${IndexBaseName} write took ${(t1 - t0)/1000} us")

    }
  }


  def writeIndexFileAndCommit2(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: ByteArrayOutputStream): Unit = {

    val bbuf = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(bbuf)

    var offset = 0L
    out2.writeLong(offset)
    for (length <- lengths) {
      offset += length
      out2.writeLong(offset)
    }
    out2.flush()
    out2.close()

//    val dataFile = getDataFile(shuffleId, mapId)
//    logTrace(s"writeIndexFileAndCommit using real data file: ${dataFile}")
//    logTrace(s"writeIndexFileAndCommit temp data file is: ${dataTmp}")

    val DataBaseName = "shuffle_" + shuffleId.toString() + "_" + mapId.toString() + ".data"
    val IndexBaseName = "shuffle_" + shuffleId.toString() + "_" + mapId.toString() + ".index"

    val BM = BM1 //if ((mapId % 2) == 0) BM1 else BM2

    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      // TODO: S: cheaper to just write again over RDMA vs reading to check each time?
//      logTrace(s"writeIndexFileAndCommit first successful map output write for ${dataFile}")

//      val t8 = System.nanoTime()
      val indexByteArray = bbuf.toByteArray()
//      val t9 = System.nanoTime()
//    logTrace(s"bbuf index conversion took ${(t9 - t8)/1000} us")

//      logTrace(s"RDMA sent index for ${IndexBaseName}")
//      val t0 = System.nanoTime()

      BM.write(IndexBaseName, indexByteArray, indexByteArray.length)
//      val t1 = System.nanoTime()

//      logTrace(s"RDMA sent index for ${IndexBaseName} complete")

      if (dataTmp != null) {
//        logTrace(s"RDMA sending data for ${DataBaseName}")
//        val t2 = System.nanoTime()

        val dat = dataTmp.toByteArray()
        BM.write(DataBaseName, dat, dat.length)

//        val t3 = System.nanoTime()

//        logTrace(s"RDMA sent data for ${DataBaseName} complete")
//    logTrace(s"RDMA ${DataBaseName} write took ${(t3 - t2)/1000} us")


      }
//    logTrace(s"RDMA ${IndexBaseName} write took ${(t1 - t0)/1000} us")

    }
  }



  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   * */
/*  def writeIndexFileAndCommit2(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: ByteArrayOutputStream): Unit = {

    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      // TODO: S: cheaper to just write again over RDMA vs reading to check each time?
      logTrace(s"writeIndexFileAndCommit first successful map output write for ${dataFile}")
      val indexByteArray = bbuf.toByteArray()
      logTrace(s"RDMA sent index for ${IndexBaseName}")
      BM.write(IndexBaseName, indexByteArray, indexByteArray.length)
      logTrace(s"RDMA sent index for ${IndexBaseName} complete")

      if (dataTmp != null ) {
        logTrace(s"RDMA sending data for ${DataBaseName}")
        val dat = dataTmp.toByteArray()
        BM.write(DataBaseName, dat, dat.length)
//        BM.write_file(dataTmp.toString(), DataBaseName)
        logTrace(s"RDMA sent data for ${DataBaseName} complete")
//        dataTmp.delete()
//

      }


    }

  }


*/




  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
//    logTrace(s"called getBlockData on IndexShuffleBlockResolver for ${blockId.name}")

    // TODO: do we plugin RDMA read of data here?

    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)
//    logTrace(s"Filename: $indexFile")


    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8)
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val DONE = new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
//      val forPrinting = DONE.nioByteBuffer()
//      logTrace(s"getBlockData for ${blockId} hashCode is: ${forPrinting.hashCode()}")

      DONE
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
