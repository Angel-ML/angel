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
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyPartOp
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, RouterUtils}
import io.netty.buffer.ByteBuf

import scala.util.Random

class LINEModelRandomizeAsNodes(param: RandomizeUpdateAsNodesParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val param = partParam.asInstanceOf[RandomizeAsNodesPartitionUpdateParam]
    val nodeIds = param.split.asInstanceOf[IIntKeyPartOp].getKeys
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, param)

    val rand = new Random(param.seed)
    nodeIds.foreach(nodeId => {
      val embedding = new Array[Float](param.dim)
      for (i <- 0 until param.dim) {
        embedding(i) = (rand.nextFloat() - 0.5f) / param.dim
      }
      if (param.order == 1) {
        row.set(nodeId, new LINENode(embedding, null))
      } else {
        row.set(nodeId, new LINENode(embedding, new Array[Float](param.dim)))
      }
    })
  }
}

class RandomizeAsNodesPartitionUpdateParam(matrixId: Int,
                                           partKey: PartitionKey,
                                           var split: KeyPart,
                                           var dim: Int,
                                           var order: Int,
                                           var seed: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, null, -1, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ByteBufSerdeUtils.serializeKeyPart(buf, split)
    ByteBufSerdeUtils.serializeInt(buf, dim)
    ByteBufSerdeUtils.serializeInt(buf, order)
    ByteBufSerdeUtils.serializeInt(buf, seed)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    split = ByteBufSerdeUtils.deserializeKeyPart(buf)
    dim = ByteBufSerdeUtils.deserializeInt(buf)
    order = ByteBufSerdeUtils.deserializeInt(buf)
    seed = ByteBufSerdeUtils.deserializeInt(buf)
  }

  override def bufferLen: Int =
    super.bufferLen + ByteBufSerdeUtils.INT_LENGTH * 3 + ByteBufSerdeUtils.serializedKeyPartLen(split)
}

/**
  * Function parameter
  *
  * @param matrixId embedding matrix id
  * @param dim      embedding vector dim
  * @param order    order
  * @param seed     random seed
  */
class RandomizeUpdateAsNodesParam(matrixId: Int, dim: Int, nodeIds: Array[Int], order: Int, seed: Int)
  extends UpdateParam(matrixId) {
  override def split: java.util.List[PartitionUpdateParam] = {
    val matrixMeta = PSAgentContext.get.getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys
    val splits = RouterUtils.split(matrixMeta, 0, nodeIds)
    val params = new util.ArrayList[PartitionUpdateParam](parts.length)

    splits.zipWithIndex.foreach(e => {
      if (e._1 != null && e._1.size() > 0) {
        params.add(
          new RandomizeAsNodesPartitionUpdateParam(
            matrixId, parts(e._2), splits(e._2), dim, order, seed + parts(e._2).getPartitionId))
      }
    })

    params
  }
}
