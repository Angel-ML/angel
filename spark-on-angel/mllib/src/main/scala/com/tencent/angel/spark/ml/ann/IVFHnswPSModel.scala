package com.tencent.angel.spark.ml.ann

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.output.format.SnapshotFormat
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann_core.psf.getIVFHnswVectors.{GetIVFHnswVectors, GetIVFHnswVectorsParam, GetIVFHnswVectorsResult}
import com.tencent.angel.spark.ml.ann_core.psf.getSearchResult.{GetSearchResult, GetSearchResultParam, GetSearchResultResult}
import com.tencent.angel.spark.ml.ann_core.psf.updateIVFHnswVectors.{UpdateIVFHnswVectors, UpdateIVFHnswVectorsParam}
import com.tencent.angel.spark.ml.ann_core.psf.updateSearchResult.{UpdateSearchResult, UpdateSearchResultParam}
import com.tencent.angel.spark.ml.ann_core.{IVFHnswVector, NearestNeighborsIElement, NeighborDistance}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class IVFHnswPSModel(val queryVecMatrix: PSMatrix,
                     val searchResMatrix: PSMatrix,
                     val resMatrixFinal: PSMatrix) extends Serializable {

  def initQueries(queries: RDD[(Long, Array[(Long, Array[Float])])]): Unit = {
    queries.foreachPartition { iter =>
      this.initQueries(iter.toArray)
    }
  }

  def initQueries(queries: Array[(Long, Array[(Long, Array[Float])])]): Unit = {
    // An empty data partition would produce a PSF without any split, which PS rejects
    if (queries.isEmpty) return
    val nodes = queries.map(_._1)
    val updates = queries.map(a => new IVFHnswVector(a._2.map(_._1), a._2.map(_._2)))

    val psFunc = new UpdateIVFHnswVectors(new UpdateIVFHnswVectorsParam(queryVecMatrix.id, nodes, updates))
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

  def updateSearchResultInCluster(res: Array[(Long, Array[(Long, Float)])], topK: Int): Unit = {
    if (res.isEmpty) return
    val nodes = res.map(_._1)
    val updates = res.map(a => new NearestNeighborsIElement(a._2.map(t => new NeighborDistance(t._1, t._2))))
    val psFunc = new UpdateSearchResult(new UpdateSearchResultParam(resMatrixFinal.id, nodes, updates, topK))
    searchResMatrix.asyncPsfUpdate(psFunc).get()
  }



  def getQueries(qId: Array[Long]): Long2ObjectOpenHashMap[Tuple2[Array[Long], Array[Array[Float]]]] = {
    if (qId.isEmpty) return new Long2ObjectOpenHashMap[Tuple2[Array[Long], Array[Array[Float]]]]()
    val psFunc = new GetIVFHnswVectors(new GetIVFHnswVectorsParam(queryVecMatrix.id, qId))
    val ret = queryVecMatrix.asyncPsfGet(psFunc).get().asInstanceOf[GetIVFHnswVectorsResult]
    ret.getVector
  }

  def getNearestNeighbors(qId: Array[Long], topK: Int): Long2ObjectOpenHashMap[Array[NeighborDistance]] = {
    if (qId.isEmpty) return new Long2ObjectOpenHashMap[Array[NeighborDistance]]()
    val psFunc = new GetSearchResult(new GetSearchResultParam(resMatrixFinal.id, qId))
    val ret = searchResMatrix.asyncPsfGet(psFunc).get().asInstanceOf[GetSearchResultResult]
    ret.getResKeysDis
  }

  def getNearestClusters(qId: Array[Long], topK: Int): Long2ObjectOpenHashMap[Array[NeighborDistance]] = {
    if (qId.isEmpty) return new Long2ObjectOpenHashMap[Array[NeighborDistance]]()
    val psFunc = new GetSearchResult(new GetSearchResultParam(searchResMatrix.id, qId))
    val ret = searchResMatrix.asyncPsfGet(psFunc).get().asInstanceOf[GetSearchResultResult]
    ret.getResKeysDis
  }


  def checkpoint(id: Int, names: Array[String]): Unit = {
    val saveContext = new ModelSaveContext()
    names.foreach(matrix => {
      saveContext.addMatrix(new MatrixSaveContext(matrix, classOf[SnapshotFormat].getTypeName))
    })
    PSContext.instance().checkpoint(id, saveContext)
  }

}


object IVFHnswPSModel {

  def apply(modelContext: ModelContext, edges: RDD[(Long, Array[Float])], useBalancePartition: Boolean = false,
            balancePartitionPercent: Float = 0.7f): IVFHnswPSModel = {
    val queryMatrixContext = ModelContextUtils.createMatrixContext(modelContext,
      "queryVector", RowType.T_ANY_LONGKEY_SPARSE, classOf[IVFHnswVector])
    val resMatrixContext = ModelContextUtils.createMatrixContext(modelContext,
      "searchRes", RowType.T_ANY_LONGKEY_SPARSE, classOf[NearestNeighborsIElement])

    val resMatrixContextFinal = ModelContextUtils.createMatrixContext(modelContext,
      "searchResFinal", RowType.T_ANY_LONGKEY_SPARSE, classOf[NearestNeighborsIElement])

    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.map(_._1)
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, queryMatrixContext, balancePartitionPercent)
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, resMatrixContext, balancePartitionPercent)
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, resMatrixContextFinal, balancePartitionPercent)
    }

    val queryMatrix = PSMatrix.matrix(queryMatrixContext)
    val resMatrix = PSMatrix.matrix(resMatrixContext)
    val resMatrixFinal = PSMatrix.matrix(resMatrixContextFinal)
    new IVFHnswPSModel(queryMatrix, resMatrix, resMatrixFinal)
  }
}