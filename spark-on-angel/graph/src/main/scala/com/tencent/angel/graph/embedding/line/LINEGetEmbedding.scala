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

package com.tencent.angel.graph.embedding.line

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyPartOp
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, RouterUtils}
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
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, getEmbeddingParam)

    val srcData = getEmbeddingParam.srcNodeIds
    val targetData = getEmbeddingParam.targetNodeIds
    var srcNodeIds: Array[Int] = null
    if (srcData != null) {
      srcNodeIds = srcData.asInstanceOf[IIntKeyPartOp].getKeys
    }

    var targetNodeIds: Array[Int] = null
    if (targetData != null) {
      targetNodeIds = targetData.asInstanceOf[IIntKeyPartOp].getKeys
    }

    val order = getEmbeddingParam.order

    // Get the number of nodes that need get feats
    var srcFeats: Int2ObjectOpenHashMap[Array[Float]] = null
    var targetFeats: Int2ObjectOpenHashMap[Array[Float]] = null


    if (order == 1) {
      if (srcNodeIds != null || targetNodeIds != null) {
        var len = 0
        if (srcNodeIds != null) len += srcNodeIds.length
        if (targetNodeIds != null) len += targetNodeIds.length

        srcFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      }
    } else {
      if (srcNodeIds != null) {
        srcFeats = new Int2ObjectOpenHashMap[Array[Float]](srcNodeIds.length)
      }
    }

    if (targetNodeIds != null) {
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
      if (partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats != null) {
        srcFeats.putAll(partResult.asInstanceOf[PartLINEGetEmbeddingResult].srcFeats)
      }

      if (partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats != null) {
        targetFetas.putAll(partResult.asInstanceOf[PartLINEGetEmbeddingResult].targetFeats)
      }
    }
    new LINEGetEmbeddingResult(srcFeats, targetFetas)
  }
}

class LINEGetEmbeddingResult(srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                             targetFeats: Int2ObjectOpenHashMap[Array[Float]]) extends GetResult {
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

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val srcSplits = RouterUtils.split(matrixMeta, 0, srcNodes.clone(), false)
    val targetSplits = RouterUtils.split(matrixMeta, 0, targetNodeIds, false)

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

class PartLINEGetEmbeddingResult(var part: PartitionKey, var srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                                 var targetFeats: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionGetResult {

  def this() = this(null, null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    if (srcFeats != null) {
      ByteBufSerdeUtils.serializeInt(output, srcFeats.size())

      val resIter = srcFeats.int2ObjectEntrySet().fastIterator()
      while (resIter.hasNext) {
        val entry = resIter.next()
        ByteBufSerdeUtils.serializeInt(output, entry.getIntKey)
        if (entry.getValue != null) {
          ByteBufSerdeUtils.serializeFloats(output, entry.getValue)
        } else {
          ByteBufSerdeUtils.serializeEmptyFloats(output)
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0)
    }

    if (targetFeats != null) {
      ByteBufSerdeUtils.serializeInt(output, targetFeats.size())

      val resIter = targetFeats.int2ObjectEntrySet().fastIterator()
      while (resIter.hasNext) {
        val entry = resIter.next()
        ByteBufSerdeUtils.serializeInt(output, entry.getIntKey)
        if (entry.getValue != null) {
          ByteBufSerdeUtils.serializeFloats(output, entry.getValue)
        } else {
          ByteBufSerdeUtils.serializeEmptyFloats(output)
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0)
    }
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    var len = ByteBufSerdeUtils.deserializeInt(input)
    if (len > 0) {
      srcFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      (0 until len).foreach(_ => {
        //srcFeats.put(input.readInt(), NodeUtils.deserializeFloatFromShort(input))
        val nodeId = ByteBufSerdeUtils.deserializeInt(input)
        var feats = ByteBufSerdeUtils.deserializeFloats(input)
        if (feats.length == 0) {
          feats = null
        }
        srcFeats.put(nodeId, feats)
      })
    }

    len = input.readInt()
    if (len > 0) {
      targetFeats = new Int2ObjectOpenHashMap[Array[Float]](len)
      (0 until len).foreach(_ => {
        //srcFeats.put(input.readInt(), NodeUtils.deserializeFloatFromShort(input))
        val nodeId = ByteBufSerdeUtils.deserializeInt(input)
        var feats = ByteBufSerdeUtils.deserializeFloats(input)
        if (feats.length == 0) {
          feats = null
        }
        targetFeats.put(nodeId, feats)
      })
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = ByteBufSerdeUtils.INT_LENGTH * 2

    var elemLen = 0
    if (srcFeats != null) {
      val resIter = srcFeats.int2ObjectEntrySet().fastIterator()
      var break = false
      while (resIter.hasNext && !break) {
        val entry = resIter.next()

        if (entry.getValue != null) {
          elemLen = ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.serializedFloatsLen(entry.getValue)
          break = true
        }
      }

      len += elemLen * srcFeats.size()
    }

    if (targetFeats != null) {
      val resIter = targetFeats.int2ObjectEntrySet().fastIterator()
      var break = false
      while (resIter.hasNext && !break) {
        val entry = resIter.next()
        if (entry.getValue != null) {
          elemLen = ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.serializedFloatsLen(entry.getValue)
          break = true
        }
      }

      len += elemLen * targetFeats.size()
    }

    len
  }


}

