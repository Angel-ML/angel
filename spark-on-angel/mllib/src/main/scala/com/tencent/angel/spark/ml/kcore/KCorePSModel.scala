package com.tencent.angel.spark.ml.kcore

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.PSVector

private[kcore] class KCorePSModel(val core: PSVector) extends Serializable {

  private val dim: Int = core.dimension.toInt

  def updateCoreWithActive(keys: Array[Int], cores: Array[Int]): Unit = {
    core.update(VFactory.sparseIntVector(dim, keys, cores))
  }

  def pull(nodes: Array[Int]): IntIntVector = {
    core.pull(nodes).asInstanceOf[IntIntVector]
  }

  def updateCoreWithActive(vector: IntIntVector): Unit = {
    core.update(vector)
  }
}

private[kcore] object KCorePSModel {
  def fromMaxId(maxId: Int): KCorePSModel = {
    val cores = PSVector.dense(maxId, 1, rowType = RowType.T_INT_DENSE)
    new KCorePSModel(cores)
  }
}
