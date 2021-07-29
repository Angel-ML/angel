package com.tencent.angel.graph.ops

import com.tencent.angel.exception.InvalidParameterException
import com.tencent.angel.graph.client.psf.sample.sampleedgefeats.{SampleEdgeFeat, SampleEdgeFeatParam, SampleEdgeFeatResult}
import com.tencent.angel.graph.client.psf.sample.sampleneighbor._
import com.tencent.angel.graph.client.psf.sample.samplenodefeats.{SampleNodeFeat, SampleNodeFeatParam, SampleNodeFeatResult}
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.graph.GraphModel
import com.tencent.angel.graph.data.VertexId
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class SampleOps(graph: GraphModel) extends Serializable {

  // sample neighbors by count, count <=0 means return all neighbors
  def sampleNeighbors(nodeIds: Array[VertexId], count: Int = -1): Long2ObjectOpenHashMap[Array[VertexId]] = {
    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return new Long2ObjectOpenHashMap[Array[VertexId]](0)
    }

    graph.graphMatrix.psfGet(new SampleNeighbor(new SampleNeighborParam(graph.graphMatrix.id, nodeIds, count)))
      .asInstanceOf[SampleNeighborResult].getNodeIdToSampleNeighbors
  }

  def sampleNeighborsWithType(
                               nodeIds: Array[VertexId],
                               count: Int = -1,
                               sampleType: SampleType = SampleType.NODE): (Long2ObjectOpenHashMap[Array[VertexId]], Long2ObjectOpenHashMap[Array[Int]], Long2ObjectOpenHashMap[Array[Int]]) = {
    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return (new Long2ObjectOpenHashMap[Array[VertexId]](0), new Long2ObjectOpenHashMap[Array[Int]](0), new Long2ObjectOpenHashMap[Array[Int]](0))
    }

    if(sampleType == SampleType.SIMPLE) {
      throw new InvalidParameterException("Sample with type only support type: NODE, EDGE and NODE_AND_EDGE");
    }

    val result = graph.graphMatrix.psfGet(
      new SampleNeighborWithType(new SampleNeighborWithTypeParam(graph.graphMatrix.id, nodeIds, count, sampleType)))
      .asInstanceOf[SampleNeighborResultWithType]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleNeighborsType, result.getNodeIdToSampleEdgeType)
  }

  def sampleNeighborsWithTypeWithFilter(nodeIds: Array[VertexId], filterWithNeighKeys: Array[VertexId],
                                        filterWithoutNeighKeys: Array[VertexId], count: Int = -1,
                                        sampleType: SampleType = SampleType.NODE): (
    Long2ObjectOpenHashMap[Array[VertexId]], Long2ObjectOpenHashMap[Array[Int]], Long2ObjectOpenHashMap[Array[Int]]) = {

    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return (new Long2ObjectOpenHashMap[Array[VertexId]](0), new Long2ObjectOpenHashMap[Array[Int]](0), new Long2ObjectOpenHashMap[Array[Int]](0))
    }

    if(sampleType != SampleType.NODE && sampleType != SampleType.EDGE) {
      throw new InvalidParameterException("Sample with filter only support type: NODE, EDGE");
    }

    val result = graph.graphMatrix.psfGet(
      new SampleNeighborWithFilter(
        new SampleNeighborWithFilterParam(
          graph.graphMatrix.id, nodeIds, count, sampleType, filterWithNeighKeys, filterWithoutNeighKeys)))
      .asInstanceOf[SampleNeighborResultWithType]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleNeighborsType, result.getNodeIdToSampleEdgeType)
  }

  def sampleNodeFeat(count: Int): Array[IntFloatVector] = {
    graph.graphMatrix.psfGet(new SampleNodeFeat(new SampleNodeFeatParam(graph.graphMatrix.id, count)))
      .asInstanceOf[SampleNodeFeatResult].getNodeFeats
  }

  def sampleNeighborEdgeFeat(nodeIds: Array[VertexId],
                             count: Int): (Long2ObjectOpenHashMap[Array[VertexId]],
    Long2ObjectOpenHashMap[Array[IntFloatVector]]) = {
    val result = graph.graphMatrix.psfGet(new SampleEdgeFeat(
      new SampleEdgeFeatParam(graph.graphMatrix.id, nodeIds, count)))
      .asInstanceOf[SampleEdgeFeatResult]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleEdgeFeats)
  }

}
