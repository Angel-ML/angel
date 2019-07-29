package com.tencent.angel.spark.ml.embedding.line2

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.NEModelRandomize.{RandomizePartitionUpdateParam, RandomizeUpdateParam}

import scala.collection.JavaConversions._
import scala.util.Random

class LINEModelRandomize(param: RandomizeUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId,
      partParam.getPartKey.getPartitionId).asInstanceOf[RowBasedPartition]
    if (part != null) {
      val ff = partParam.asInstanceOf[RandomizePartitionUpdateParam]
      update(part, partParam.getPartKey, ff.partDim, ff.dim, ff.order, ff.seed)
    }
  }

  private def update(part: RowBasedPartition, key: PartitionKey, partDim: Int, dim: Int, order: Int, seed:Int): Unit = {
    val row: ServerIntAnyRow = part.getRow(0).asInstanceOf[ServerIntAnyRow]
    println(s"random seed in init=${seed}")

    val rand = new Random(seed)
    (row.getStartCol until row.getEndCol).map(colId => {

      val embedding = new Array[Float](dim)
      for (i <- 0 until dim) {
        embedding(i) = (rand.nextFloat() - 0.5f) / dim//colId.toFloat / 10000//
      }
      if(order == 1) {
        row.set(colId.toInt, new LINENode(embedding, null))
      } else {
        row.set(colId.toInt, new LINENode(embedding, new Array[Float](dim)))
      }
    })
  }

  class RandomizeUpdateParam(matrixId: Int, partDim: Int, dim: Int, order: Int, seed: Int)
    extends UpdateParam(matrixId) {
    override def split: java.util.List[PartitionUpdateParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
        new RandomizePartitionUpdateParam(matrixId, part, partDim, dim, order, seed + part.getPartitionId)
      }
    }
  }
}
