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
import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils
import com.tencent.angel.utils.Sort
import io.netty.buffer.ByteBuf

import scala.collection.JavaConversions._

/**
  * A PS function to init neighbor table and edge weights
  *
  * @param param function params
  */
class InitEdgeWeight(param: InitEgdeWeightParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    // Get the matrix partition that store neighbor table on PS
    val initParam = partParam.asInstanceOf[PartInitEgdeWeightParam]
    val matrix = psContext.getMatrixStorageManager.getMatrix(initParam.getMatrixId)
    val part = matrix.getPartition(initParam.getPartKey.getPartitionId)
    val row = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]

    initParam.srcNodes.zip(initParam.edges).foreach(e => {
      val node = row.get(e._1).asInstanceOf[LINENode]
      if (e._2 != null) {
        node.setNeighbors(e._2.dstNodes)
        node.setWeights(e._2.weights)
      }
    })
  }
}

/**
  * Function parameters
  *
  * @param matrixId neighbor table matrix id
  * @param srcNodes src nodes
  * @param edges    node neighbors and weights
  */
class InitEgdeWeightParam(matrixId: Int, srcNodes: Array[Int], edges: Array[EdgeWeightPairs]) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)
    Sort.quickSort(srcNodes, edges, 0, srcNodes.length - 1)

    val srcIndicesViews = RowUpdateSplitUtils.split(srcNodes, parts, true)

    val partParams = new util.ArrayList[PartitionUpdateParam](srcIndicesViews.size())
    srcIndicesViews.foreach(e => {
      partParams.add(new PartInitEgdeWeightParam(matrixId, e._1, srcNodes, e._2.getStart, e._2.getEnd, edges))
    })

    partParams
  }
}

class PartInitEgdeWeightParam(matrixId: Int, partKey: PartitionKey, var srcNodes: Array[Int],
                              startPos: Int, endPos: Int, var edges: Array[EdgeWeightPairs])
  extends PartitionUpdateParam(matrixId, partKey) {

  def this() = this(-1, null, null, -1, -1, null)

  override def serialize(output: ByteBuf): Unit = {
    super.serialize(output)
    output.writeInt(endPos - startPos)


    for (i <- startPos until endPos) {
      // Src node id
      output.writeInt(srcNodes(i))

      // Dst node id and weight
      if (edges(i) != null) {
        output.writeBoolean(true)
        edges(i).serialize(output)
      } else {
        // Empty
        output.writeBoolean(false)
      }
    }
  }

  override def deserialize(input: ByteBuf): Unit = {
    super.deserialize(input)
    val len = input.readInt()
    srcNodes = new Array[Int](len)
    edges = new Array[EdgeWeightPairs](len)

    for (i <- 0 until len) {
      srcNodes(i) = input.readInt()
      if (input.readBoolean()) {
        edges(i) = new EdgeWeightPairs()
        edges(i).deserialize(input)
      }
    }
  }

  override def bufferLen: Int = {
    var len = super.bufferLen()
    len += 4

    for (i <- 0 until srcNodes.length) {
      len += 4
      if (edges(i) != null) {
        len += (4 + edges(i).bufferLen())
      } else {
        len += 4
      }
    }

    len
  }
}

class EdgeWeightPairs(var dstNodes: Array[Int], var weights: Array[Float]) extends Serialize {
  def this() = this(null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    output.writeInt(dstNodes.length)
    dstNodes.foreach(e => output.writeInt(e))
    weights.foreach(e => output.writeFloat(e))
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    val len = input.readInt()
    dstNodes = new Array[Int](len)
    weights = new Array[Float](len)

    for (i <- 0 until len) {
      dstNodes(i) = input.readInt()
    }

    for (i <- 0 until len) {
      weights(i) = input.readFloat()
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = 4 + dstNodes.length * (4 + 4)
}