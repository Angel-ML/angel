package com.tencent.angel.spark.ml.graph.clusterrank

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.Param
import com.tencent.angel.spark.ml.graph.data.VertexId
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.psf.clusterrank._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Number of edges in out-neighbors of vertices
  * @param param
  */
class NeighborEdgesModel(val param: Param) extends Serializable {
  var psMatrix: PSMatrix = _

  def init(data: RDD[(VertexId, Long)]): Unit = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName("clusterrank.neighbor-edges")
    mc.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
    mc.setRowNum(1)
    mc.setColNum(param.maxIndex)
    mc.setMaxColNumInBlock(param.maxIndex / param.psPartNum)

    psMatrix = PSMatrix.matrix(mc)

    data.foreachPartition { iter =>
      BatchIter(iter, param.batchSize).foreach { pairs =>
        val nodeIdToNumEdges = new Long2LongOpenHashMap(pairs.length)
        pairs.foreach { case (nodeId, numEdges) =>
          nodeIdToNumEdges.put(nodeId, numEdges)
        }
        val psfunc = new InitNumNeighborEdgesFunc(new InitNumNeighborEdgesParam(psMatrix.id, nodeIdToNumEdges))
        psMatrix.asyncPsfUpdate(psfunc).get()

        nodeIdToNumEdges.clear()
        println(s"push number of edges in neighbors of ${pairs.length} nodes")
      }
    }
  }

  def getNumEdges(nodeIds: Array[VertexId]): Long2LongOpenHashMap = {
    psMatrix.psfGet(new GetNumNeighborEdgesFunc(new GetNumNeighborEdgesParam(psMatrix.id, nodeIds)))
      .asInstanceOf[GetNumNeighborEdgesResult].getNodeIdToNumEdges
  }

}


object NeighborEdgesModel {
  def create(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int): NeighborEdgesModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum)
    new NeighborEdgesModel(param)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

}
