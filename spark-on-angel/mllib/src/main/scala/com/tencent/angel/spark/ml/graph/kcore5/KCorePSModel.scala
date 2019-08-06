package com.tencent.angel.spark.ml.graph.kcore5

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.sona.models.PSVector
import com.tencent.angel.sona.models.impl.PSVectorImpl
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

private[kcore5]
class KCorePSModel(var inMsgs: PSVector,
                   var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension

  def initMsgs(msgs: Vector): Unit =
    inMsgs.update(msgs)

  def readMsgs(nodes: Array[Long]): LongIntVector =
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]

  def readAllMsgs(): LongIntVector =
    inMsgs.pull().asInstanceOf[LongIntVector]

  def writeMsgs(msgs: Vector): Unit =
    outMsgs.update(msgs)

  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)

  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }

}

private[kcore5] object KCorePSModel {

  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): KCorePSModel = {
    val matrix = new MatrixContext("cores", 2, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    // use balance partition
    if (useBalancePartition)
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix)

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("cores").getId
    new KCorePSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType))
  }

}
