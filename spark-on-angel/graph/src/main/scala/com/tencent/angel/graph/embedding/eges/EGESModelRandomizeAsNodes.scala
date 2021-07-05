package com.tencent.angel.graph.embedding.eges

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

class EGESModelRandomizeAsNodes(param: WeightsRandomizeUpdateAsNodesParam) extends UpdateFunc(param) {
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
      row.set(nodeId, new EmbeddingNode(embedding))
    })
  }
}

class RandomizeAsNodesPartitionUpdateParam(matrixId: Int,
                                           partKey: PartitionKey,
                                           var split: KeyPart,
                                           var dim: Int,
                                           var seed: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, null, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ByteBufSerdeUtils.serializeKeyPart(buf, split)
    ByteBufSerdeUtils.serializeInt(buf, dim)
    ByteBufSerdeUtils.serializeInt(buf, seed)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    split = ByteBufSerdeUtils.deserializeKeyPart(buf)
    dim = ByteBufSerdeUtils.deserializeInt(buf)
    seed = ByteBufSerdeUtils.deserializeInt(buf)
  }

  override def bufferLen: Int =
    super.bufferLen + ByteBufSerdeUtils.INT_LENGTH * 2 + ByteBufSerdeUtils.serializedKeyPartLen(split)
}

/**
 * Function parameter
 *
 * @param matrixId embedding matrix id
 * @param dim      embedding vector dim
 * @param seed     random seed
 */
class WeightsRandomizeUpdateAsNodesParam(matrixId: Int, dim: Int, nodeIds: Array[Int], seed: Int)
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
            matrixId, parts(e._2), splits(e._2), dim, seed + parts(e._2).getPartitionId))
      }
    })

    params
  }
}