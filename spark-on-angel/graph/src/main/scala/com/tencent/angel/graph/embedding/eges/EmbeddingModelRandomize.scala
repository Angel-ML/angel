package com.tencent.angel.graph.embedding.eges

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * A PS function to initialize the embedding vector on PS
  *
  * @param param function parameters
  */
class EmbeddingModelRandomize(param: WeightsRandomizeUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId,
      partParam.getPartKey.getPartitionId).asInstanceOf[RowBasedPartition]
    if (part != null) {
      val ff = partParam.asInstanceOf[RandomizePartitionUpdateParam]
      update(part, partParam.getPartKey, ff.dim, ff.seed, ff.numSlots)
    }
  }

  private def update(part: RowBasedPartition, key: PartitionKey, dim: Int, seed: Int, numSlots: Int): Unit = {
    val row: ServerIntAnyRow = part.getRow(0).asInstanceOf[ServerIntAnyRow]
    println(s"random seed in init=${seed}")

    val rand = new Random(seed)
    (row.getStartCol until row.getEndCol).foreach(colId => {

      val embedding = new Array[Float](dim)
      if (numSlots == 1) {
        for (i <- 0 until dim) {
          embedding(i) = (rand.nextFloat() - 0.5f) / dim
        }
      } else {
        for (i <- 0 until (dim / numSlots)) {
          embedding(i) = (rand.nextFloat() - 0.5f) / (dim / numSlots)
        }
        for (i <- (dim / numSlots) until dim) {
          embedding(i) = 0.0f
        }
      }
      row.set(colId.toInt, new EmbeddingNode(embedding))
    })
  }
}

class RandomizePartitionUpdateParam(matrixId: Int,
                                    partKey: PartitionKey,
                                    var dim: Int,
                                    var seed: Int,
                                    var numSlots: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, -1, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    buf.writeInt(dim)
    buf.writeInt(seed)
    buf.writeInt(numSlots)

  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    this.dim = buf.readInt()
    this.seed = buf.readInt()
    this.numSlots = buf.readInt()
  }

  override def bufferLen: Int = super.bufferLen + 12
}

/**
  * Function parameter
  *
  * @param matrixId embedding matrix id
  * @param dim      embedding vector dim
  * @param seed     random seed
  */
class WeightsRandomizeUpdateParam(matrixId: Int, dim: Int, seed: Int, numSlots: Int)
  extends UpdateParam(matrixId) {
  override def split: java.util.List[PartitionUpdateParam] = {
    PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
      new RandomizePartitionUpdateParam(matrixId, part, dim, seed + part.getPartitionId, numSlots)
    }
  }
}
