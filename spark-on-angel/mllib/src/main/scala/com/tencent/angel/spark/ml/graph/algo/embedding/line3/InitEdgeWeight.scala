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

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.common.{ByteBufSerdeUtils, StreamSerdeUtils}
import com.tencent.angel.graph.common.psf.param.{AnyKeysUpdateParam, IntKeysUpdateParam}
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.update.base.{GeneralPartUpdateParam, PartitionUpdateParam, UpdateFunc}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.matrix.transport.router.operator.{IAnyKeyAnyValuePartOp, IIntKeyAnyValuePartOp}
import io.netty.buffer.ByteBuf

/**
  * A PS function to init neighbor table and edge weights
  *
  * @param param function params
  */
class InitEdgeWeight(param: AnyKeysUpdateParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    // Get the matrix partition that store neighbor table on PS
    val initParam = partParam.asInstanceOf[GeneralPartUpdateParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, initParam)

    val srcNodes = initParam.getKeyValuePart.asInstanceOf[IAnyKeyAnyValuePartOp].getKeys
    val targetAndWeights = initParam.getKeyValuePart.asInstanceOf[IAnyKeyAnyValuePartOp].getValues

    row.startWrite()
    try {
      srcNodes.zip(targetAndWeights).foreach(e => {
        val node = row.get(e._1).asInstanceOf[LINENode]
        if (e._2 != null) {
          node.setNeighbors(e._2.asInstanceOf[EdgeWeightPairs].dstNodes)
          node.setWeights(e._2.asInstanceOf[EdgeWeightPairs].weights)
        }
      })
    } finally {
      row.endWrite()
    }
  }
}

class EdgeWeightPairs(var dstNodes: Array[ReadOnlyByteArray], var weights: Array[Float]) extends IElement {
  def this() = this(null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    ByteBufSerdeUtils.serializeInt(output, dstNodes.length)
    dstNodes.foreach(e => e.serialize(output))
    ByteBufSerdeUtils.serializeFloats(output, weights)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    val len = ByteBufSerdeUtils.deserializeInt(input)
    dstNodes = new Array[ReadOnlyByteArray](len)
    (0 until len).foreach(index => {
      dstNodes(index) = new ReadOnlyByteArray()
      dstNodes(index).deserialize(input)
    }
    )
    weights = ByteBufSerdeUtils.deserializeFloats(input)
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = ByteBufSerdeUtils.INT_LENGTH
    dstNodes.foreach(e => len += e.bufferLen())
    len += ByteBufSerdeUtils.serializedFloatsLen(weights)
    len
  }


  override def deepClone(): AnyRef = new EdgeWeightPairs(dstNodes.clone(), weights.clone())

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: DataOutputStream): Unit = {
    StreamSerdeUtils.serializeInt(output, dstNodes.length)
    dstNodes.foreach(e => e.serialize(output))
    StreamSerdeUtils.serializeFloats(output, weights)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: DataInputStream): Unit = {
    val len = StreamSerdeUtils.deserializeInt(input)
    dstNodes = new Array[ReadOnlyByteArray](len)
    (0 until len).foreach(index => {
      dstNodes(index) = new ReadOnlyByteArray()
      dstNodes(index).deserialize(input)
    }
    )
    weights = StreamSerdeUtils.deserializeFloats(input)
  }

  override def dataLen(): Int = bufferLen()
}