package com.tencent.angel.spark.ml.ann

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.model.output.format.SnapshotFormat
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann_core.{ANNVector, NearestNeighborsIElement, NeighborDistance}
import com.tencent.angel.spark.ml.ann_core.psf.getSearchResult.{GetSearchResult, GetSearchResultParam, GetSearchResultResult}
import com.tencent.angel.spark.ml.ann_core.psf.getVectors.{GetVectors, GetVectorsParam, GetVectorsResult}
import com.tencent.angel.spark.ml.ann_core.psf.updateSearchResult.{UpdateSearchResult, UpdateSearchResultParam}
import com.tencent.angel.spark.ml.ann_core.psf.updateVectors.{UpdateVectors, UpdateVectorsParam}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class ANNPSModel(val queryVecMatrix: PSMatrix,
                 val searchResMatrix: PSMatrix) extends Serializable {
  
  def initQueries(queries: RDD[(Long, Array[Float])]): Unit = {
    queries.foreachPartition{ iter =>
      this.initQueries(iter.toArray)
    }
  }
  
  def initQueries(queries: Array[(Long, Array[Float])]): Unit = {
    // An empty data partition would produce a PSF without any split, which PS rejects
    if (queries.isEmpty) return
    val nodes = queries.map(_._1)
    val updates = queries.map(a => new ANNVector(a._2))

    val psFunc = new UpdateVectors(new UpdateVectorsParam(queryVecMatrix.id, nodes, updates))
    queryVecMatrix.asyncPsfUpdate(psFunc).get()
    println(s"init ${queries.length} queries in total")
  }

  def updateSearchResult(res: Array[(Long, Array[(Long, Float)])], topK: Int): Unit = {
    if (res.isEmpty) return
    val nodes = res.map(_._1)
    val updates = res.map(a => new NearestNeighborsIElement(a._2.map(t => new NeighborDistance(t._1, t._2))))
    
    val psFunc = new UpdateSearchResult(new UpdateSearchResultParam(searchResMatrix.id, nodes, updates, topK))
    searchResMatrix.asyncPsfUpdate(psFunc).get()
  }
  
  def getQueries(qId: Array[Long]): Long2ObjectOpenHashMap[Array[Float]] = {
    if (qId.isEmpty) return new Long2ObjectOpenHashMap[Array[Float]]()
    val psFunc = new GetVectors(new GetVectorsParam(queryVecMatrix.id, qId))
    val ret = queryVecMatrix.asyncPsfGet(psFunc).get().asInstanceOf[GetVectorsResult]
    ret.getVector
  }
  
  def getQueries(): Long2ObjectOpenHashMap[Array[Float]] = {
    null
  }
  
  def getNearestNeighbors(qId: Array[Long], topK: Int): Long2ObjectOpenHashMap[Array[NeighborDistance]] = {
    if (qId.isEmpty) return new Long2ObjectOpenHashMap[Array[NeighborDistance]]()
    val psFunc = new GetSearchResult(new GetSearchResultParam(searchResMatrix.id, qId))
    val ret = searchResMatrix.asyncPsfGet(psFunc).get().asInstanceOf[GetSearchResultResult]
    ret.getResKeysDis
  }
  
  def getNearestNeighbors(topK: Int): Long2ObjectOpenHashMap[Array[NeighborDistance]] = {
    null
  }
  
  def checkpoint(id: Int, names: Array[String]): Unit = {
    val saveContext = new ModelSaveContext()
    names.foreach(matrix => {
      saveContext.addMatrix(new MatrixSaveContext(matrix, classOf[SnapshotFormat].getTypeName))
    })
    
    PSContext.instance().checkpoint(id, saveContext)
  }
  
}

object ANNPSModel {
  
  def apply(modelContext: ModelContext, edges: RDD[(Long, Array[Float])], useBalancePartition: Boolean = false,
            balancePartitionPercent: Float = 0.7f): ANNPSModel = {
    val queryMatrixContext = ModelContextUtils.createMatrixContext(modelContext,
      "queryVector", RowType.T_ANY_LONGKEY_SPARSE, classOf[ANNVector])
    val resMatrixContext = ModelContextUtils.createMatrixContext(modelContext,
      "searchRes", RowType.T_ANY_LONGKEY_SPARSE, classOf[NearestNeighborsIElement])
    
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.map(_._1)
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, queryMatrixContext, balancePartitionPercent)
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, resMatrixContext, balancePartitionPercent)
    }
    
    val queryMatrix = PSMatrix.matrix(queryMatrixContext)
    val resMatrix = PSMatrix.matrix(resMatrixContext)
    
    new ANNPSModel(queryMatrix, resMatrix)
    
  }
}
