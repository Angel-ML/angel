package com.tencent.angel.spark.ml.graph.ops

import com.tencent.angel.graph.client.psf.get.getedgefeats.{GetEdgeFeats, GetEdgeFeatsResult}
import com.tencent.angel.graph.client.psf.get.getedgetypes.GetEdgeTypes
import com.tencent.angel.graph.client.psf.get.getedgeweights.GetEdgeWeights
import com.tencent.angel.graph.client.psf.get.getlabels.GetLabels
import com.tencent.angel.graph.client.psf.get.getneighbors.{GetNeighbors, GetNeighborsResult}
import com.tencent.angel.graph.client.psf.get.getnodefeats.{GetNodeFeats, GetNodeFeatsResult}
import com.tencent.angel.graph.client.psf.get.getnodetypes.GetNodeTypes
import com.tencent.angel.graph.client.psf.get.utils.{GetFloatArrayAttrsResult, GetIntArrayAttrsResult, GetNodeAttrsParam}
import com.tencent.angel.graph.data.{Feature, Neighbor}
import com.tencent.angel.spark.ml.graph.GraphModel
import com.tencent.angel.spark.ml.graph.data.VertexId
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class GetOps(graph: GraphModel) extends Serializable {

  def getNodeFeats(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Feature] = {
    graph.graphMatrix.psfGet(new GetNodeFeats(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetNodeFeatsResult].getnodeIdToFeats()
  }

  def getEdgeFeats(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Feature]] = {
    graph.graphMatrix.psfGet(new GetEdgeFeats(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetEdgeFeatsResult].getNodeIdToContents
  }

  def getEdgeTypes(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Int]] = {
    graph.graphMatrix.psfGet(new GetEdgeTypes(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetIntArrayAttrsResult].getNodeIdToContents
  }

  def getNodeTypes(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Int]] = {
    graph.graphMatrix.psfGet(new GetNodeTypes(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetIntArrayAttrsResult].getNodeIdToContents
  }

  def getEdgeWeights(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Float]] = {
    graph.graphMatrix.psfGet(new GetEdgeWeights(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetFloatArrayAttrsResult].getNodeIdToContents
  }

  def getLabels(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Float]] = {
    graph.graphMatrix.psfGet(new GetLabels(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetFloatArrayAttrsResult].getNodeIdToContents
  }

  def getNeighbors(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Neighbor] = {
    graph.graphMatrix.psfGet(new GetNeighbors(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetNeighborsResult].getNodeIdToContents
  }

}
