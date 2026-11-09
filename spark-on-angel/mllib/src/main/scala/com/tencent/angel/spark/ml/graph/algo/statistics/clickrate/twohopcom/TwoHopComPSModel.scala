package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.twohopcom

import com.tencent.angel.spark.ml.graph.{NeighborTableModel, Param}
import com.tencent.angel.ml.math2.vector.{LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import it.unimi.dsi.fastutil.longs.{Long2LongOpenHashMap, Long2ObjectOpenHashMap}
import org.apache.spark.rdd.RDD

class TwoHopComPSModel(val neighborTable: NeighborTableModel, val userComMsgs: PSVector) extends Serializable {
  val dim: Long = userComMsgs.dimension
  
  def initLongNeighborTable(data: RDD[(Long, Array[Long])], nodes: RDD[Long] = null): Unit = {
    neighborTable.initLongNeighbor(data, nodes)
  }
  
  def getLongNeighborTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val neighborsMap = neighborTable.sampleLongNeighbors(nodeIds, -1)
    neighborsMap
  }
  
  def initUserCom(msgs: Vector): Unit = {
    userComMsgs.update(msgs)
  }
  
  def getUserCom(nodes: Array[Long]): LongLongVector = {
    userComMsgs.pull(nodes).asInstanceOf[LongLongVector]
  }
  
  def checkpoint(): Unit = {
    neighborTable.psMatrix.checkpoint()
  }
}

object TwoHopComPSModel{
  def apply(maxId: Long, batchSize: Int, pullBatchSize: Int, psPartNum: Int, nodes: RDD[Long], minId: Long = 0L,
            matrixName: String = "neighborTable", useBalancePartition: Boolean = false, balancePartitionPercent: Float = 0.7f): TwoHopComPSModel = {
    val param = new Param(maxId, batchSize, pullBatchSize, psPartNum, minId, matrixName=matrixName, useBalancePartition=useBalancePartition)
    val neighborTable = new NeighborTableModel(param)
    
    val matrix = new MatrixContext("labels", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(nodes, maxId, psPartNum, matrix, balancePartitionPercent)
    }
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("labels").getId
  
    val userCom = new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType)
    
    new TwoHopComPSModel(neighborTable, userCom)
  }
}
