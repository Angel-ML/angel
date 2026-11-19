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
/**
 * Author: zhitaowang (zhitaowang@tecent.com)
 * Created At: 2021/03/01
 */

package com.tencent.angel.spark.ml.graph.algo.embedding.bge

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.exception.AngelException
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.element.{FloatArrayElement, IElement}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.{KeyValuePart, RouterUtils}
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyAnyValuePartOp
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.util.Random

class BGEAdjust(var param: BGEAdjustParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val adjustParam = partParam.asInstanceOf[PartBGEAdjustParam]
    val extraInitial = adjustParam.extraInitial  // 外部初始化
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, adjustParam)

    //row.startWrite()
    try {
      if (adjustParam.inputUpdates != null) {
        val inputData = adjustParam.inputUpdates.asInstanceOf[IIntKeyAnyValuePartOp]
        val inputNodes = inputData.getKeys
        val inputUpdates = inputData.getValues

        if(inputNodes != null) {
          if (!extraInitial) {
            inputNodes.zip(inputUpdates).foreach(e => {
              inc(row.get(e._1).asInstanceOf[BGENode].getSrcFeats, e._2.asInstanceOf[FloatArrayElement].getData)
            })
          } else {
            val rand = new Random(System.currentTimeMillis())
            inputNodes.zip(inputUpdates).foreach(e => {
              val ele = row.get(e._1)
              if (ele == null) {
                val dim = e._2.asInstanceOf[FloatArrayElement].getData.length
                val tgtEmbedding = new Array[Float](dim)
                for (i <- 0 until dim) {
                  tgtEmbedding(i) = (rand.nextFloat() - 0.5f) / dim
                }
                row.set(e._1, new BGENode(null, tgtEmbedding))
              }
              row.get(e._1).asInstanceOf[BGENode].setSrcFeats(e._2.asInstanceOf[FloatArrayElement].getData)
            })
          }
        }
      }

      if (adjustParam.outputUpdates != null) {
        val outputData = adjustParam.outputUpdates.asInstanceOf[IIntKeyAnyValuePartOp]
        val outputNodes = outputData.getKeys
        val outputUpdates = outputData.getValues

        if (outputNodes != null) {
          if (!extraInitial) {
            outputNodes.zip(outputUpdates).foreach(e => {
              inc(row.get(e._1).asInstanceOf[BGENode].getTgtFeats, e._2.asInstanceOf[FloatArrayElement].getData)
            })
          } else {
            val rand = new Random(System.currentTimeMillis())
            outputNodes.zip(outputUpdates).foreach(f = e => {
              val ele = row.get(e._1)
              if (ele == null) {
                val dim = e._2.asInstanceOf[FloatArrayElement].getData.length
                val srcEmbedding = new Array[Float](dim)
                for (i <- 0 until dim) {
                  srcEmbedding(i) = (rand.nextFloat() - 0.5f) / dim
                }
                row.set(e._1, new BGENode(srcEmbedding, null))
              }
              row.get(e._1).asInstanceOf[BGENode].setTgtFeats(e._2.asInstanceOf[FloatArrayElement].getData)
            })
          }
        }
      }
    } finally {
      //row.endWrite()
    }
  }

  def inc(y: Array[Float], x: Array[Float]) = {
    for (i <- y.indices) {
      y(i) += x(i)
    }
  }
}

class BGEAdjustParam(matrixId: Int, srcUpdates: Int2ObjectOpenHashMap[Array[Float]],
                     tgtUpdates: Int2ObjectOpenHashMap[Array[Float]], extraInitial: Boolean=false) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys
    val partParams = new util.ArrayList[PartitionUpdateParam](parts.length)

    val inputSplits = if (srcUpdates != null && !srcUpdates.isEmpty) {
      splitIntFloatsMap(matrixMeta, srcUpdates)
    } else null

    val outputSplits = if (tgtUpdates != null && !tgtUpdates.isEmpty) {
      splitIntFloatsMap(matrixMeta, tgtUpdates)
    } else null

    if (inputSplits != null && outputSplits != null) {
      for (index <- 0 until parts.length) {
        if ((inputSplits(index) != null && inputSplits(index).size() > 0)
          || (outputSplits(index) != null && outputSplits(index).size() > 0)) {
          partParams.add(
            new PartBGEAdjustParam(
              matrixId, parts(index), extraInitial, inputSplits(index), outputSplits(index)))
        }
      }
    } else if (inputSplits != null && outputSplits == null) {
      for (index <- 0 until parts.length) {
        if (inputSplits(index) != null && inputSplits(index).size() > 0) {
          partParams.add(
            new PartBGEAdjustParam(
              matrixId, parts(index), extraInitial, inputSplits(index), null))
        }
      }
    } else if (inputSplits == null && outputSplits != null) {
      for (index <- 0 until parts.length) {
        if (outputSplits(index) != null && outputSplits(index).size() > 0) {
          partParams.add(
            new PartBGEAdjustParam(
              matrixId, parts(index), extraInitial, null, outputSplits(index)))
        }
      }
    } else {
      throw new AngelException("Invalid parameters, both inputUpdates and outputUpdates are null")
    }

    partParams
  }

  def splitIntFloatsMap(matrixMeta: MatrixMeta, data: Int2ObjectOpenHashMap[Array[Float]]): Array[KeyValuePart] = {
    val nodeIds: Array[Int] = new Array[Int](data.size())
    val updates: Array[IElement] = new Array[IElement](data.size())

    if (data != null && data.size() > 0) {
      val iter = data.entrySet().iterator()
      var index = 0
      while (iter.hasNext) {
        val entry = iter.next()
        nodeIds(index) = entry.getKey
        updates(index) = new FloatArrayElement(entry.getValue)
        index += 1
      }

      RouterUtils.split(matrixMeta, 0, nodeIds, updates)
    } else {
      new Array[KeyValuePart](matrixMeta.getPartitionNum)
    }
  }
}

class PartBGEAdjustParam(matrixId: Int,
                         part: PartitionKey,
                         var extraInitial: Boolean,
                         var inputUpdates: KeyValuePart,
                         var outputUpdates: KeyValuePart) extends PartitionUpdateParam(matrixId, part) {

  def this() = this(-1, null, false, null, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ByteBufSerdeUtils.serializeBoolean(buf, extraInitial)

    if (inputUpdates != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyValuePart(buf, inputUpdates)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }

    if (outputUpdates != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyValuePart(buf, outputUpdates)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    extraInitial = ByteBufSerdeUtils.deserializeBoolean(buf)

    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      inputUpdates = ByteBufSerdeUtils.deserializeKeyValuePart(buf)
    }

    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      outputUpdates = ByteBufSerdeUtils.deserializeKeyValuePart(buf)
    }
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.BOOLEN_LENGTH
    len += ByteBufSerdeUtils.BOOLEN_LENGTH
    if (inputUpdates != null) {
      len += ByteBufSerdeUtils.serializedKeyValuePartLen(inputUpdates)
    }

    len += ByteBufSerdeUtils.BOOLEN_LENGTH
    if (outputUpdates != null) {
      len += ByteBufSerdeUtils.serializedKeyValuePartLen(outputUpdates)
    }
    len
  }
}

