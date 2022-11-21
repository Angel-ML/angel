package com.tencent.angel.graph.rank.linerank

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntLongVector, LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.psf.linerank.GetNodes
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

private[linerank]
class LineRankPSModel(value: PSVector) extends Serializable {

  final val matrixId = value.poolId
  final val dim = value.dimension

  def sendMsgs(msgs: Vector): Unit = {
    value.increment(msgs)
  }

  def resetValues(): Unit = value.reset

  def readValues(nodes: Array[Long]): LongFloatVector =
    value.pull(nodes).asInstanceOf[LongFloatVector]

  def getNodes(partitionIds: Array[Int]): Array[Long] = {
    val func = new GetNodes(value.poolId, partitionIds)
    val res = value.psfGet(func).asInstanceOf[GetRowResult].getRow.asInstanceOf[IntLongVector]
      .getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
    res
  }

  def numNodes(): Long = VectorUtils.size(value)
}

private[linerank]
object LineRankPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
                 useBalancePartition: Boolean, balancePartitionPercent: Float): LineRankPSModel = {
    val matrix = new MatrixContext("linerank", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY)
    matrix.setPartitionerClass(classOf[ColumnRangePartitioner])

    if (useBalancePartition)
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)

    PSContext.instance().createMatrix(matrix)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("linerank").getId
    new LineRankPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType))
  }

  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float): LineRankPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_FLOAT_SPARSE_LONGKEY)

    val psMatrix = PSMatrix.matrix(matrix)
    new LineRankPSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType))
  }

}
