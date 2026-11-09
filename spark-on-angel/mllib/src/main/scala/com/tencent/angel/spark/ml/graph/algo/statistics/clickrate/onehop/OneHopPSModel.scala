package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehop

import com.tencent.angel.spark.ml.graph.{NeighborTableModel, Param}
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class OneHopPSModel(val neighborTable: NeighborTableModel) extends Serializable {
  
  def initLongNeighborTable(data: RDD[(Long, Array[Long])], nodes: RDD[Long] = null): Unit = {
    neighborTable.initLongNeighbor(data, nodes)
  }
  
  def getLongNeighborTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val neighborsMap = neighborTable.sampleLongNeighbors(nodeIds, -1)
    neighborsMap
  }
  
  def checkpoint(): Unit = {
    neighborTable.psMatrix.checkpoint()
  }
}

object OneHopPSModel {
  def apply(maxId: Long, batchSize: Int, pullBatchSize: Int, psPartNum: Int, minId: Long = 0L,
            matrixName: String = "neighborTable", useBalancePartition: Boolean = false): OneHopPSModel = {
    val param = new Param(maxId, batchSize, pullBatchSize, psPartNum, minId, matrixName=matrixName, useBalancePartition=useBalancePartition)
    val neighborTable = new NeighborTableModel(param)
    new OneHopPSModel(neighborTable)
  }
}


