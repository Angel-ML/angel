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
package com.tencent.angel.spark.ml.graph.algo.embedding.line2

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

import scala.collection.convert.ImplicitConversions._

class LINEGetSrcEmbedding(param: LINEGetSrcEmbeddingParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getEmbeddingParam = partParam.asInstanceOf[PartLINEGetSrcEmbeddingParam]
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, getEmbeddingParam)

    val srcData = getEmbeddingParam.srcNodeIds
    var srcNodeIds: Array[Int] = null
    if (srcData != null) {
      srcNodeIds = srcData.asInstanceOf[IIntKeyPartOp].getKeys
    }

    // Get the number of nodes that need get feats
    var srcFeats: Int2ObjectOpenHashMap[Array[Float]] = null

    if (srcNodeIds != null) {
      srcFeats = new Int2ObjectOpenHashMap[Array[Float]](srcNodeIds.length)
    }

    // Get feats for source nodes
    if (srcNodeIds != null) {
      for (nodeId <- srcNodeIds) {
        srcFeats.put(nodeId, row.get(nodeId).asInstanceOf[LINENode].getInputFeats)
      }
    }

    new PartLINEGetSrcEmbeddingResult(getEmbeddingParam.getPartKey, srcFeats)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val srcFeats = new Int2ObjectOpenHashMap[Array[Float]](param.srcNodeNum)

    for (partResult <- partResults) {
      if (partResult.asInstanceOf[PartLINEGetSrcEmbeddingResult].srcFeats != null) {
        srcFeats.putAll(partResult.asInstanceOf[PartLINEGetSrcEmbeddingResult].srcFeats)
      }
    }
    new LINEGetSrcEmbeddingResult(srcFeats)
  }
}

class LINEGetSrcEmbeddingResult(srcFeats: Int2ObjectOpenHashMap[Array[Float]]) extends GetResult {
  def getResult: (Int2ObjectOpenHashMap[Array[Float]]) = srcFeats
}

class LINEGetSrcEmbeddingParam(matrixId: Int, srcNodes: Array[Int]) extends GetParam(matrixId) {
  var srcNodeNum = 0

  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionGetParam] = {
    srcNodeNum = srcNodes.length

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val srcSplits = RouterUtils.split(matrixMeta, 0, srcNodes.clone(), false)

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- 0 until parts.length) {
      if (srcSplits(index) != null && srcSplits(index).size() > 0){
        partParams.add(
          new PartLINEGetSrcEmbeddingParam(
            matrixId, parts(index), srcSplits(index)))
      }
    }

    partParams
  }
}

class PartLINEGetSrcEmbeddingParam(matrixId: Int,
                                part: PartitionKey,
                                var srcNodeIds: KeyPart) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    // Src nodes
    if (srcNodeIds != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyPart(buf, srcNodeIds)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    // Src node
    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      srcNodeIds = ByteBufSerdeUtils.deserializeKeyPart(buf)
    }
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.BOOLEN_LENGTH * 2
    if (srcNodeIds != null) {
      len += ByteBufSerdeUtils.serializedKeyPartLen(srcNodeIds)
    }
    len
  }
}

class PartLINEGetSrcEmbeddingResult(var part: PartitionKey,
                                    var srcFeats: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionGetResult {

  def this() = this(null, null)

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

    len
  }


}