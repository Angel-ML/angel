/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.graph.data.NodeUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import scala.collection.convert.ImplicitConversions._

class EmbeddingGet(param: EmbeddingGetParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getEmbeddingParam = partParam.asInstanceOf[PartEmbeddingGetParam]
    val matrix = psContext.getMatrixStorageManager.getMatrix(getEmbeddingParam.getMatrixId)
    val part = matrix.getPartition(getEmbeddingParam.getPartKey.getPartitionId)
    val row = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]

    val nodeIds = getEmbeddingParam.nodeIds

    // Get the number of nodes that need get feats
    var embeddings:Int2ObjectOpenHashMap[Array[Float]] = null

    // Get embeddings for nodes
    if (nodeIds != null) {
      embeddings = new Int2ObjectOpenHashMap[Array[Float]](nodeIds.length)
      for (nodeId <- nodeIds) {
        embeddings.put(nodeId, row.get(nodeId).asInstanceOf[EmbeddingNode].getFeats)
      }
    }

    new PartEmbeddingGetResult(getEmbeddingParam.getPartKey, embeddings)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val embeddings = new Int2ObjectOpenHashMap[Array[Float]](param.nodeNum)

    for (partResult <- partResults) {
      if(partResult.asInstanceOf[PartEmbeddingGetResult].embeddings != null) {
        embeddings.putAll(partResult.asInstanceOf[PartEmbeddingGetResult].embeddings)
      }
    }
    new EmbeddingGetResult(embeddings)
  }
}

class EmbeddingGetResult(embeddings: Int2ObjectOpenHashMap[Array[Float]]) extends GetResult {
  def getResult: Int2ObjectOpenHashMap[Array[Float]] = embeddings
}

class EmbeddingGetParam(matrixId: Int, nodes: Array[Int]) extends GetParam(matrixId) {
  var nodeNum = 0

  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionGetParam] = {
    nodeNum = nodes.length

    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)

    // Sort and split the node ids
    val nodeIndicesViews = RowUpdateSplitUtils.split(nodes.clone(), parts, false)

    val partToParams = new util.HashMap[PartitionKey, PartEmbeddingGetParam](nodeIndicesViews.size())

    // set node splits
    val nodeSplitIter = nodeIndicesViews.entrySet().iterator()
    while (nodeSplitIter.hasNext) {
      val entry = nodeSplitIter.next()
      partToParams.put(entry.getKey, new PartEmbeddingGetParam(matrixId, entry.getKey,
        entry.getValue.getIndices, entry.getValue.getStart, entry.getValue.getEnd))
    }

    val partParams = new util.ArrayList[PartitionGetParam]
    partParams.addAll(partToParams.values())
    partParams
  }
}

class PartEmbeddingGetParam(matrixId: Int, part: PartitionKey, var nodeIds: Array[Int],
                            var start: Int, var end: Int) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, null, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    if (nodeIds != null) {
      buf.writeInt(end - start)
      for (i <- start until end) {
        buf.writeInt(nodeIds(i))
      }
    } else {
      buf.writeInt(0)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    val nodeNum = buf.readInt()
    if (nodeNum > 0) {
      nodeIds = new Array[Int](nodeNum)
      for (i <- 0 until nodeNum) {
        nodeIds(i) = buf.readInt()
      }
    }
  }

  override def bufferLen(): Int = {
    super.bufferLen() + 4 + (end - start) * 4
  }
}

class PartEmbeddingGetResult(var part: PartitionKey,
                             var embeddings: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionGetResult {

  def this() = this(null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    if(embeddings != null) {
      output.writeInt(embeddings.size())
      val resIter = embeddings.int2ObjectEntrySet().fastIterator()
      while(resIter.hasNext) {
        val entry = resIter.next()
        output.writeInt(entry.getIntKey)
        NodeUtils.serializeShortFromFloat(entry.getValue, output)
      }
    } else {
      output.writeInt(0)
    }
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    val len = input.readInt()
    if(len > 0) {
      embeddings = new Int2ObjectOpenHashMap[Array[Float]](len)
      for (_ <- 0 until len) {
        embeddings.put(input.readInt(), NodeUtils.deserializeFloatFromShort(input))
      }
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = 4
    var elemLen = 0
    if(embeddings != null) {
      val resIter = embeddings.int2ObjectEntrySet().fastIterator()
      var break = false
      while(resIter.hasNext && !break) {
        val entry = resIter.next()
        elemLen = 4 + NodeUtils.dataLenShortFromFloat(entry.getValue)
        break = true
      }

      len += elemLen * embeddings.size()
    }
    len
  }
}
