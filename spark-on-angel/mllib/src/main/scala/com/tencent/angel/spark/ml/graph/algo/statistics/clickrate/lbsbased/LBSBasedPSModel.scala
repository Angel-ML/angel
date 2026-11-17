package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.lbsbased

import com.tencent.angel.spark.ml.graph.{NeighborTableModel, Param}
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class LBSBasedPSModel(val userAdsTable: NeighborTableModel) extends Serializable {
  def initLongAdTables(data: RDD[(Long, Array[Long])], nodes: RDD[Long] = null): Unit = {
    userAdsTable.initLongNeighbor(data, nodes)
  }
  
  def getLongAdTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val userAdsMap = userAdsTable.sampleLongNeighbors(nodeIds, -1)
    userAdsMap
  }

  def checkpoint(): Unit = {
    userAdsTable.psMatrix.checkpoint()
  }
  
}

object LBSBasedPSModel{
  def apply(maxId: Long, batchSize: Int, pullBatchSize: Int, psPartNum: Int, minId: Long = 0L,
            matrixName: String = "ads", useBalancePartition: Boolean = false): LBSBasedPSModel = {
    val param = new Param(maxId, batchSize, pullBatchSize, psPartNum, minId, matrixName=matrixName, useBalancePartition=useBalancePartition)
    val neighborTable = new NeighborTableModel(param)
    new LBSBasedPSModel(neighborTable)
  }
}
