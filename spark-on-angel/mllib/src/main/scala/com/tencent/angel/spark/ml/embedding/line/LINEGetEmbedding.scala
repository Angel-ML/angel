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
package com.tencent.angel.spark.ml.embedding.line

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

import scala.collection.JavaConversions._

class LINEGetEmbedding(param: LINEGetEmbeddingParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getEmbeddingParam = partParam.asInstanceOf[PartLINEGetEmbeddingParam]
    val matrix = psContext.getMatrixStorageManager.getMatrix(getEmbeddingParam.getMatrixId)
    val part = matrix.getPartition(getEmbeddingParam.getPartKey.getPartitionId)
    val row = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]

    val srcNodeIds = getEmbeddingParam.srcNodeIds
    val targetNodeIds = getEmbeddingParam.targetNodeIds
    val order = getEmbeddingParam.order

    // Get the number of nodes that need get feats
    var srcFeats:Int2ObjectOpenHashMap[Array[Float]] = null
    var targetFeats:Int2ObjectOpenHashMap[Array[Float]] = null



    if(order == 1) {
      if(srcNodeIds != null || targetNodeIds != null) {
        var len = 0
        if(srcNodeIds != null) len += srcNodeIds.length
        if(targetNodeIds != null) len += targetNodeIds.length

        srcFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      }
    } else {
      if (srcNodeIds != null) {
        srcFeats = new Int2ObjectOpenHashMap[Array[Float]](srcNodeIds.length)
      }
    }

    if(targetNodeIds != null) {
      targetFeats = new Int2ObjectOpenHashMap[Array[Float]](targetNodeIds.length)
    }

    // Get feats for source nodes
    if (srcNodeIds != null) {
      for (nodeId <- srcNodeIds) {
        srcFeats.put(nodeId, row.get(nodeId).asInstanceOf[LINENode].getInputFeats)
      }
    }

    // Get feats for target nodes(dest nodes and negative sample nodes)
    // We use srcFeats to store all node(src nodes and target nodes) features in order == 1
    if (order == 1) {
      // If order == 1, just get from input feats of LINENode
      if (targetNodeIds != null) {
        for (nodeId <- targetNodeIds) {
          srcFeats.put(nodeId, row.get(nodeId).asInstanceOf[LINENode].getInputFeats)
        }
      }
    } else {
      // If order == 2, just get from output feats of LINENode
      // Use targetFeats to store target node features
      if (targetNodeIds != null) {
        for (nodeId <- targetNodeIds) {
          targetFeats.put(nodeId, row.get(nodeId).asInstanceOf[LINENode].getOutputFeats)
        }
      }
    }

    new PartLINEGetEmbeddingResult(getEmbeddingParam.getPartKey, srcFeats, targetFeats)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val srcFeats = new Int2ObjectOpenHashMap[Array[Float]](param.srcNodeNum)
    val targetFetas = new Int2ObjectOpenHashMap[Array[Float]](param.targetNodeNum)

    for (partResult <- partResults) {
      if(partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats != null) {
        srcFeats.putAll(partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats)
      }

      if(partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats != null) {
        targetFetas.putAll(partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats)
      }
    }
    new LINEGetEmbeddingResult(srcFeats, targetFetas)
  }
}

class LINEGetEmbeddingResult(srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                             targetFeats:Int2ObjectOpenHashMap[Array[Float]]) extends GetResult {
  def getResult: (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = (srcFeats, targetFeats)
}

class LINEGetEmbeddingParam(matrixId: Int, srcNodes: Array[Int], dstNodes: Array[Int],
                            negativeSamples: Array[Array[Int]], order: Int, negative: Int) extends GetParam(matrixId) {
  var srcNodeNum = 0
  var targetNodeNum = 0

  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionGetParam] = {
    srcNodeNum = srcNodes.length

    // Merge the dest nodes and negative sample nodes
    var offset: Int = 0
    val targetNodeIds = new Array[Int](dstNodes.length + negative * negativeSamples.length)
    Array.copy(dstNodes, 0, targetNodeIds, offset, dstNodes.length)
    offset += dstNodes.length
    for (i <- negativeSamples.indices) {
      Array.copy(negativeSamples(i), 0, targetNodeIds, offset, negativeSamples(i).length)
      offset += negativeSamples(i).length
    }

    targetNodeNum = targetNodeIds.length

    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)

    // Sort and split the node ids
    val srcIndicesViews = RowUpdateSplitUtils.split(srcNodes.clone(), parts, false)
    val targetIndicesViews = RowUpdateSplitUtils.split(targetNodeIds, parts, false)

    val partToParams = new util.HashMap[PartitionKey, PartLINEGetEmbeddingParam](targetIndicesViews.size())

    // Merge the src node splits and target node splits
    val srcIter = srcIndicesViews.entrySet().iterator()
    while (srcIter.hasNext) {
      val entry = srcIter.next()
      partToParams.put(entry.getKey, new PartLINEGetEmbeddingParam(matrixId, entry.getKey,
        entry.getValue.getIndices, entry.getValue.getStart, entry.getValue.getEnd,
        null, -1, -1, order))
    }

    val targetIter = targetIndicesViews.entrySet().iterator()
    while (targetIter.hasNext) {
      val entry = targetIter.next()
      val partParam = partToParams.get(entry.getKey)
      if (partParam == null) {
        partToParams.put(entry.getKey, new PartLINEGetEmbeddingParam(matrixId, entry.getKey,
          null, -1, -1,
          entry.getValue.getIndices, entry.getValue.getStart, entry.getValue.getEnd,
          order))
      } else {
        partParam.targetNodeIds = entry.getValue.getIndices
        partParam.targetStart = entry.getValue.getStart
        partParam.targetEnd = entry.getValue.getEnd
      }
    }

    val partParams = new util.ArrayList[PartitionGetParam]
    partParams.addAll(partToParams.values())
    partParams
  }
}

class PartLINEGetEmbeddingParam(matrixId: Int, part: PartitionKey, var srcNodeIds: Array[Int],
                                var srcStart: Int, var srcEnd: Int, var targetNodeIds: Array[Int],
                                var targetStart: Int, var targetEnd: Int, var order: Int) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, null, -1, -1, null, -1, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    // Src nodes
    if (srcNodeIds != null) {
      buf.writeInt(srcEnd - srcStart)
      for (i <- srcStart until srcEnd) {
        buf.writeInt(srcNodeIds(i))
      }
    } else {
      buf.writeInt(0)
    }

    // Target nodes
    if (targetNodeIds != null) {
      buf.writeInt(targetEnd - targetStart)
      for (i <- targetStart until targetEnd) {
        buf.writeInt(targetNodeIds(i))
      }
    } else {
      buf.writeInt(0)
    }

    // Order
    buf.writeInt(order)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    // Src node
    val srcNodeNum = buf.readInt()
    if (srcNodeNum > 0) {
      srcNodeIds = new Array[Int](srcNodeNum)
      for (i <- 0 until srcNodeNum) {
        srcNodeIds(i) = buf.readInt()
      }
    }

    // Target node
    val targetNodeNum = buf.readInt()
    if (targetNodeNum > 0) {
      targetNodeIds = new Array[Int](targetNodeNum)
      for (i <- 0 until targetNodeNum) {
        targetNodeIds(i) = buf.readInt()
      }
    }

    // Order
    order = buf.readInt()
  }

  override def bufferLen(): Int = {
    super.bufferLen() + 4 + (srcEnd - srcStart) * 4 + 4 + (targetEnd - targetStart) * 4 + 4
  }
}

class PartLINEGetEmbeddingResult(var part: PartitionKey,  var srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                                 var targetFeats: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionGetResult {

  def this() = this(null, null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    if(srcFeats != null) {
      output.writeInt(srcFeats.size())

      val resIter = srcFeats.int2ObjectEntrySet().fastIterator()
      while(resIter.hasNext) {
        val entry = resIter.next()
        output.writeInt(entry.getIntKey)
        NodeUtils.serialize(entry.getValue, output)
      }
    } else {
      output.writeInt(0)
    }

    if(targetFeats != null) {
      output.writeInt(targetFeats.size())

      val resIter = targetFeats.int2ObjectEntrySet().fastIterator()
      while(resIter.hasNext) {
        val entry = resIter.next()
        output.writeInt(entry.getIntKey)
        NodeUtils.serialize(entry.getValue, output)
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
    var len = input.readInt()
    if(len > 0) {
      srcFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      for (i <- 0 until len) {
        srcFeats.put(input.readInt(), NodeUtils.deserializeFloats(input))
      }
    }

    len = input.readInt()
    if(len > 0) {
      targetFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      for (i <- 0 until len) {
        targetFeats.put(input.readInt(), NodeUtils.deserializeFloats(input))
      }
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = 8

    var elemLen = 0
    if(srcFeats != null) {
      val resIter = srcFeats.int2ObjectEntrySet().fastIterator()
      var break = false
      while(resIter.hasNext && !break) {
        val entry = resIter.next()
        elemLen = 4 + NodeUtils.dataLen(entry.getValue)
        break = true
      }

      len += elemLen * srcFeats.size()
    }

    if(targetFeats != null) {
      val resIter = targetFeats.int2ObjectEntrySet().fastIterator()
      var break = false
      while(resIter.hasNext && !break) {
        val entry = resIter.next()
        elemLen = 4 + NodeUtils.dataLen(entry.getValue)
        break = true
      }

      len += elemLen * targetFeats.size()
    }

    len
  }
}

