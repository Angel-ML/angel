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
package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.data.NodeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyKeyPartOp
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, RouterUtils}
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import scala.collection.convert.ImplicitConversions._

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
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, getEmbeddingParam)

    val srcData = getEmbeddingParam.srcNodeIds
    val targetData = getEmbeddingParam.targetNodeIds
    var srcNodeIds: Array[IElement] = null
    if (srcData != null) {
      srcNodeIds = srcData.asInstanceOf[IAnyKeyPartOp].getKeys
    }

    var targetNodeIds: Array[IElement] = null
    if (targetData != null) {
      targetNodeIds = targetData.asInstanceOf[IAnyKeyPartOp].getKeys
    }

    val order = getEmbeddingParam.order

    // Get the number of nodes that need get feats
    var srcFeats: Object2ObjectOpenHashMap[IElement, Array[Float]] = null
    var targetFeats: Object2ObjectOpenHashMap[IElement, Array[Float]] = null

    if (order == 1) {
      if (srcNodeIds != null || targetNodeIds != null) {
        var len = 0
        if (srcNodeIds != null) len += srcNodeIds.length
        if (targetNodeIds != null) len += targetNodeIds.length

        srcFeats = new Object2ObjectOpenHashMap[IElement, Array[Float]](len)
      }
    } else {
      if (srcNodeIds != null) {
        srcFeats = new Object2ObjectOpenHashMap[IElement, Array[Float]](srcNodeIds.length)
      }
    }

    if (targetNodeIds != null) {
      targetFeats = new Object2ObjectOpenHashMap[IElement, Array[Float]](targetNodeIds.length)
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
          if(row.get(nodeId) == null) {
            println(s"============row node is null, id = ${nodeId}")
          }
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
    val srcFeats = new Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]](param.srcNodeNum)
    val targetFetas = new Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]](param.targetNodeNum)

    for (partResult <- partResults) {
      if (partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats != null) {
        val iter = partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats.object2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next
          srcFeats.put(entry.getKey.asInstanceOf[ReadOnlyByteArray], entry.getValue)
        }
      }

      if (partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats != null) {
        val iter = partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats.object2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next
          targetFetas.put(entry.getKey.asInstanceOf[ReadOnlyByteArray], entry.getValue)
        }
      }
    }
    new LINEGetEmbeddingResult(srcFeats, targetFetas)
  }
}

class LINEGetEmbeddingResult(srcFeats: Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]],
                             targetFeats: Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]]) extends GetResult {
  def getResult = (srcFeats, targetFeats)
}

class LINEGetEmbeddingParam(matrixId: Int, srcNodes: Array[ReadOnlyByteArray], dstNodes: Array[ReadOnlyByteArray],
                            negativeSamples: Array[Array[ReadOnlyByteArray]], order: Int, negative: Int) extends GetParam(matrixId) {
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
    val targetNodeIds = new Array[ReadOnlyByteArray](dstNodes.length + negative * negativeSamples.length)
    Array.copy(dstNodes, 0, targetNodeIds, offset, dstNodes.length)
    offset += dstNodes.length
    for (i <- 0 until negativeSamples.length) {
      Array.copy(negativeSamples(i), 0, targetNodeIds, offset, negativeSamples(i).length)
      offset += negativeSamples(i).length
    }

    targetNodeNum = targetNodeIds.length

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val srcSplits = RouterUtils.split(matrixMeta, 0, srcNodes.clone().map(e => e.asInstanceOf[IElement]), false)
    val targetSplits = RouterUtils.split(matrixMeta, 0, targetNodeIds.map(e => e.asInstanceOf[IElement]), false)

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- (0 until parts.length)) {
      if ((srcSplits(index) != null && srcSplits(index).size() > 0)
        || (targetSplits(index) != null && targetSplits(index).size() > 0)) {
        partParams.add(
          new PartLINEGetEmbeddingParam(
            matrixId, parts(index), srcSplits(index), targetSplits(index), order))
      }
    }

    partParams
  }
}

class PartLINEGetEmbeddingParam(matrixId: Int,
                                part: PartitionKey,
                                var srcNodeIds: KeyPart,
                                var targetNodeIds: KeyPart,
                                var order: Int) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, null, null, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    // Src nodes
    if (srcNodeIds != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyPart(buf, srcNodeIds)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }

    // Target nodes
    if (targetNodeIds != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyPart(buf, targetNodeIds)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }

    // Order
    buf.writeInt(order)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    // Src node
    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      srcNodeIds = ByteBufSerdeUtils.deserializeKeyPart(buf)
    }

    // Target node
    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      targetNodeIds = ByteBufSerdeUtils.deserializeKeyPart(buf)
    }

    // Order
    order = buf.readInt()
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.BOOLEN_LENGTH * 2
    if (srcNodeIds != null) {
      len += ByteBufSerdeUtils.serializedKeyPartLen(srcNodeIds)
    }

    if (targetNodeIds != null) {
      len += ByteBufSerdeUtils.serializedKeyPartLen(targetNodeIds)
    }
    len
  }
}

class PartLINEGetEmbeddingResult(var part: PartitionKey,
                                 var srcFeats: Object2ObjectOpenHashMap[IElement, Array[Float]],
                                 var targetFeats: Object2ObjectOpenHashMap[IElement, Array[Float]]) extends PartitionGetResult {

  def this() = this(null, null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    if (srcFeats != null) {
      output.writeInt(srcFeats.size())

      val resIter = srcFeats.object2ObjectEntrySet().fastIterator()
      while (resIter.hasNext) {
        val entry = resIter.next()
        entry.getKey.serialize(output)
        //NodeUtils.serializeShortFromFloat(entry.getValue, output)
        NodeUtils.serialize(entry.getValue, output)
      }
    } else {
      output.writeInt(0)
    }

    if (targetFeats != null) {
      output.writeInt(targetFeats.size())

      val resIter = targetFeats.object2ObjectEntrySet().fastIterator()
      while (resIter.hasNext) {
        val entry = resIter.next()
        entry.getKey.serialize(output)
        //NodeUtils.serializeShortFromFloat(entry.getValue, output)
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
    if (len > 0) {
      srcFeats = new Object2ObjectOpenHashMap[IElement, Array[Float]](len)
      for (i <- 0 until len) {
        //srcFeats.put(input.readInt(), NodeUtils.deserializeFloatFromShort(input))
        val key = new ReadOnlyByteArray()
        key.deserialize(input)
        srcFeats.put(key, NodeUtils.deserializeFloats(input))
      }
    }

    len = input.readInt()
    if (len > 0) {
      targetFeats = new Object2ObjectOpenHashMap[IElement, Array[Float]](len)
      for (i <- 0 until len) {
        val key = new ReadOnlyByteArray()
        key.deserialize(input)
        //targetFeats.put(input.readInt(), NodeUtils.deserializeFloatFromShort(input))
        targetFeats.put(key, NodeUtils.deserializeFloats(input))
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
    if (srcFeats != null) {
      val resIter = srcFeats.object2ObjectEntrySet().fastIterator()
      var break = false
      while (resIter.hasNext && !break) {
        val entry = resIter.next()
        //elemLen = 4 + NodeUtils.dataLenShortFromFloat(entry.getValue)
        elemLen = entry.getKey.bufferLen() + NodeUtils.dataLen(entry.getValue)
        break = true
      }

      len += elemLen * srcFeats.size()
    }

    if (targetFeats != null) {
      val resIter = targetFeats.object2ObjectEntrySet().fastIterator()
      var break = false
      while (resIter.hasNext && !break) {
        val entry = resIter.next()
        //elemLen = 4 + NodeUtils.dataLenShortFromFloat(entry.getValue)
        elemLen = entry.getKey.bufferLen() + NodeUtils.dataLen(entry.getValue)
        break = true
      }

      len += elemLen * targetFeats.size()
    }

    len
  }


}

