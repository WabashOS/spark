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

package org.apache.spark.network.netty

import java.io._
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
//import java.util.concurrent.Executors
//import concurrent.ExecutionContext
import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, NioManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

import ucb.remotebuf._

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 */
private[spark] class NettyBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    bindAddress: String,
    override val hostName: String,
    _port: Int,
    numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)
  private val BM = new RemoteBuf.BufferManager()

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new SaslServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new SaslClientBootstrap(transportConf, conf.getAppId, securityManager,
        securityManager.isSaslEncryptionEnabled()))
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    logTrace(s"All Blocks: ${blockIds.mkString(" ")}")

    // hacky - also need to change IndexShuffleBlockResolver's RDMA_SHUFFLE
    val RDMA_SHUFFLE = true

    // TODO: can we just take out any block that starts with "shuffle_" for now?
    // and put it in our own list
    //
    // then manually obtain those blocks from /nscratch/ for now and call 
    // the listener on them one by one
    //
    // The rest of the function will run as expected for the other blocks

    val other_blockIds = 
      if (RDMA_SHUFFLE)
        blockIds.filter(x => !(x contains "shuffle_"))
      else
        blockIds

    if (RDMA_SHUFFLE) {
      logTrace(s"Other Blocks: ${other_blockIds.mkString(" ")}")
    } else {
      logTrace(s"Blocks: ${other_blockIds.mkString(" ")}")
    }
  
    if (RDMA_SHUFFLE) {
      val shuffleBlocks = blockIds.filter(x => x contains "shuffle_")
      logTrace(s"Shuffle Blocks: ${shuffleBlocks.mkString(" ")}")

//      val executorService = Executors.newFixedThreadPool(2)
//      val executionContext = ExecutionContext.fromExecutorService(executorService)

      /* code adapted from IndexShuffleBlockResolver getBlockData */
      for (aShuffleBlock <- shuffleBlocks) {
        val f = Future {

          val t0 = System.nanoTime()

          val shuffleId = aShuffleBlock.split("_")(1)
          val blockId = aShuffleBlock.split("_")(2)
          val reduceId = aShuffleBlock.split("_")(3).toInt

          val baseName = s"shuffle_${shuffleId}_${blockId}"
          val IndexBaseName = baseName + ".index"
          val DataBaseName = baseName + ".data"

          // TODO: S: can we hardcode 1608?
          val IndexFileSize = 1608 //BM.get_read_alloc(IndexBaseName)
          logTrace(s"RDMA got index alloc for ${baseName}.index file size as: ${IndexFileSize}")
          val indexFileRDMAIn = new Array[Byte](IndexFileSize)
          val t2 = System.nanoTime()
          BM.read(IndexBaseName, indexFileRDMAIn, IndexFileSize)
          val t3 = System.nanoTime()
          logTrace(s"RDMA got read response from server for ${baseName}.index")
          val in = new DataInputStream(new ByteArrayInputStream(indexFileRDMAIn))

          try {
            ByteStreams.skipFully(in, reduceId * 8)
            val offset = in.readLong()
            val nextOffset = in.readLong()

            // TODO get rid of the toInt s
            val DataFileSize = (nextOffset - offset).toInt
            logTrace(s"For ${baseName}.data file size is: ${DataFileSize}")
            val dataFileRDMAIn = new Array[Byte](DataFileSize)
            val t4 = System.nanoTime()
          
            BM.read_offset(DataBaseName, dataFileRDMAIn, DataFileSize, offset.toInt)
            val t5 = System.nanoTime()
           logTrace(s"Raw RDMA read for ${aShuffleBlock} data took ${(t5 - t4)/1000000} ms")

            logTrace(s"RDMA got read response from server for ${baseName}.data")
            val DONE = new NioManagedBuffer(ByteBuffer.wrap(dataFileRDMAIn))

            listener.onBlockFetchSuccess(aShuffleBlock, DONE)
          } finally {
            in.close()
          }

          val t1 = System.nanoTime() 
          logTrace(s"Request for ${aShuffleBlock} took ${(t1 - t0)/1000000} ms")
          logTrace(s"Raw RDMA read for ${aShuffleBlock} index took ${(t3 - t2)/1000000} ms")

        } /*(executionContext)*/
      }
      /* end code adapted from getBlockData */
    }

    // TODO do we need to do anything special here if there are no non-shuffle
    // blocks?
    if (!(other_blockIds.length == 0)) {
      try {
        val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
          override def createAndStart(other_blockIds: Array[String], listener: BlockFetchingListener) {
            val client = clientFactory.createClient(host, port)
            new OneForOneBlockFetcher(client, appId, execId, other_blockIds.toArray, listener).start()
          }
        }

        //create block fetch starter


        val maxRetries = transportConf.maxIORetries()
        if (maxRetries > 0) {
          // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
          // a bug in this code. We should remove the if statement once we're sure of the stability.
          new RetryingBlockFetcher(transportConf, blockFetchStarter, other_blockIds, listener).start()
        } else {
          blockFetchStarter.createAndStart(other_blockIds, listener)
        }
      } catch {
        case e: Exception =>
          logError("Exception while beginning fetchBlocks", e)
          other_blockIds.foreach(listener.onBlockFetchFailure(_, e))
      }
    }
  }

  override def port: Int = server.getPort

  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // Convert or copy nio buffer into array in order to serialize it.
    val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, array).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success((): Unit)
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }
}
