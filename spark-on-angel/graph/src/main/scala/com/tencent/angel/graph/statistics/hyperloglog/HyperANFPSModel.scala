package com.tencent.angel.graph.statistics.hyperloglog

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.psf.hyperanf.{GetHyperLogLogResult, GetReadCounter, GetWriteCounter, GetWriteCounterResult, HyperLogLogElement, InitHyperEdge, InitHyperVertex, UpdateReadCounter, UpdateWriteCounter}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class HyperANFPSModel(matrix: PSMatrix) extends Serializable {

  final val matrixId = matrix.id
  final val dim = matrix.columns

  def checkpoint(): Unit = {
    matrix.checkpoint()
  }

  def init(nodes: Array[Long], nodeTags: Array[Int], p: Int, sp: Int, seed: Long): Unit = {
    val func = new InitHyperVertex(matrix.id, p, sp, nodes, nodeTags, seed)
    matrix.psfUpdate(func).get()
  }

  def initEdge(nodes: Array[Long], p: Int, sp: Int, seed: Long): Unit = {
    val func = new InitHyperEdge(matrix.id, nodes, p, sp, seed)
    matrix.psfUpdate(func).get()
  }

  def getReadCounter(nodes: Array[Long]): Long2ObjectOpenHashMap[HyperLogLogPlus] = {
    val func = new GetReadCounter(matrix.id, nodes)
    matrix.psfGet(func).asInstanceOf[GetHyperLogLogResult].getResults
  }

  def getWriteCounter(nodes: Array[Long]): Long2ObjectOpenHashMap[(HyperLogLogPlus, java.lang.Long)] = {
    val func = new GetWriteCounter(matrix.id, nodes)
    matrix.psfGet(func).asInstanceOf[GetWriteCounterResult].getResults
  }

  def sendMsgs(updates: Long2ObjectOpenHashMap[HyperLogLogPlus], p: Int, sp: Int, seed: Long): Unit = {
    val func = new UpdateWriteCounter(matrix.id, updates, p, sp, seed)
    matrix.psfUpdate(func).get()
  }

  def updateReadCounter(): Unit = {
    val func = new UpdateReadCounter(matrix.id)
    matrix.psfUpdate(func).get()
  }

}

object HyperANFPSModel {

  def fromMinMax(modelContext: ModelContext,
                 index: RDD[Long],
                 useBalancePartition: Boolean,
                 percent: Float): HyperANFPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[HyperLogLogElement])

    if (useBalancePartition && modelContext.isUseRangePartition)
      LoadBalancePartitioner.partition(index, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, percent)

    val psMatrix = PSMatrix.matrix(matrix)
    new HyperANFPSModel(new PSMatrixImpl(psMatrix.id, matrix.getName, 1, modelContext.getMaxNodeId, matrix.getRowType))
  }

}