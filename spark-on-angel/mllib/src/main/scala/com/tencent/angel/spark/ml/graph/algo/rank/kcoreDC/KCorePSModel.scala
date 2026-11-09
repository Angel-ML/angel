package com.tencent.angel.spark.ml.graph.algo.rank.kcoreDC

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.ml.psf.kcore.readTag.{ReadTag, ReadTagResult}
import com.tencent.angel.spark.ml.psf.kcore.updateTag.UpdateTag
import com.tencent.angel.spark.ml.psf.kcore.{GetCores, GetCoresResult}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.spark.rdd.RDD

class KCorePSModel(var inMsgs: PSVector,
                   var outMsgs: PSVector,
                   var cores: PSVector,
                   val staticCores: PSVector,
                   psMatrix: PSMatrix) extends Serializable {
  
  val dim: Long = cores.dimension
  val matrixId: Int = cores.poolId
  
  def initStaticCores(values: Vector): Unit = {
    assert(staticCores != null, s"no static cores provided")
    staticCores.update(values)
  }
  
  
  def readStaticCores(nodes: Array[Long]): LongIntVector = {
    assert(staticCores != null, s"no static cores provided")
    staticCores.pull(nodes).asInstanceOf[LongIntVector]
  }
  
  
  def initCores(values: Vector): Unit =
    cores.update(values)
  
  def readCores(nodes: Array[Long]): LongIntVector =
    cores.pull(nodes).asInstanceOf[LongIntVector]
  
  def readValidCores(nodes: Array[Long], threshold: Int): Long2IntOpenHashMap = {
    cores.psfGet(new GetCores(matrixId, nodes, threshold, cores.id)).asInstanceOf[GetCoresResult].getCores
  }
  
  def updateTag(nodes: Array[Long]): Unit = {
    outMsgs.psfUpdate(new UpdateTag(matrixId, nodes, outMsgs.id)).get()
  }
  
  def readTag(nodes: Array[Long]): Array[Long] = {
    inMsgs.psfGet(new ReadTag(matrixId, nodes, inMsgs.id)).asInstanceOf[ReadTagResult].getNodes
  }
  
  def readMsgs(nodes: Array[Long]): LongIntVector =
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]
  
  def readAllMsgs(): LongIntVector =
    inMsgs.pull().asInstanceOf[LongIntVector]
  
  def writeInMsgs(values: Vector): Unit = {
    inMsgs.update(values)
  }
  
  def writeMsgs(values: Vector): Unit =
    outMsgs.update(values)
  
  def updateCores(values: Vector): Unit =
    cores.update(values)
  
  def asyncUpdateCores(values: Vector): Unit =
    cores.asyncUpdate(values)
  
  def numCores(): Long = VectorUtils.nnz(cores)
  
  def numKeys2calc(): Long = VectorUtils.nnz(inMsgs)
  
  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)
  
  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }
  
  def resetCores(): Unit = {
    cores.reset
  }
  
  def checkpoint(n: Int): Unit = psMatrix.checkpoint(n)
}

object KCorePSModel {
  def apply(modelContext: ModelContext, edges: RDD[(Long, Long)], mode: String = "full", useBalancePartition: Boolean = false,
            balancePartitionPercent: Float = 0.7f): KCorePSModel = {
    val matrix =
      if (mode.toLowerCase == "sparse" || mode.toLowerCase == "mid") {
        ModelContextUtils.createMatrixContext(modelContext, RowType.T_INT_SPARSE_LONGKEY, 4)
      }
      else {
        ModelContextUtils.createMatrixContext(modelContext, RowType.T_INT_SPARSE_LONGKEY, 3)
      }
      
    
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.flatMap(e => Iterator(e._1, e._2))
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)
    }
    
    val psMatrix = PSMatrix.matrix(matrix)
    if (mode.toLowerCase() == "mid" || mode.toLowerCase() == "sparse") {
      new KCorePSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 1, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 2, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 3, modelContext.getMaxNodeId, matrix.getRowType), psMatrix)
    }
    else {
      new KCorePSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 1, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 2, modelContext.getMaxNodeId, matrix.getRowType), null, psMatrix)
    }
    
  }
  
}