package com.tencent.angel.spark.ml.graph.ops

import com.tencent.angel.exception.InvalidParameterException
import com.tencent.angel.graph.client.psf.sample.sampleedgefeats.{SampleEdgeFeat, SampleEdgeFeatParam, SampleEdgeFeatResult}
import com.tencent.angel.graph.client.psf.sample.sampleneighbor._
import com.tencent.angel.graph.client.psf.sample.samplenodefeats.{SampleNodeFeat, SampleNodeFeatParam, SampleNodeFeatResult}
import com.tencent.angel.graph.data.{Feature, Neighbor, NeighborStorageType}
import com.tencent.angel.spark.ml.graph.GraphModel
import com.tencent.angel.spark.ml.graph.data.VertexId
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class SampleOps(graph: GraphModel) extends Serializable {

  // sample neighbors by count, count <=0 means return all neighbors
  def sampleNeighbors(nodeIds: Array[VertexId], count: Int = -1, method: SampleMethod = SampleMethod.RANDOM): Long2ObjectOpenHashMap[Neighbor] = {
    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return new Long2ObjectOpenHashMap[Neighbor](0)
    }

    graph.graphMatrix.psfGet(new SampleNeighbor(new SampleNeighborParam(graph.graphMatrix.id, nodeIds, count, method, NeighborStorageType.LONGARRAY)))
      .asInstanceOf[SampleNeighborResult].getNodeIdToSampleNeighbors
  }

  def sampleNeighborsWithType(
                               nodeIds: Array[VertexId],
                               count: Int = -1,
                               sampleType: SampleType = SampleType.NODE,
                               sampleMethod: SampleMethod = SampleMethod.RANDOM): (Long2ObjectOpenHashMap[Neighbor], Long2ObjectOpenHashMap[Array[Int]], Long2ObjectOpenHashMap[Array[Int]]) = {
    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return (new Long2ObjectOpenHashMap[Neighbor](0), new Long2ObjectOpenHashMap[Array[Int]](0), new Long2ObjectOpenHashMap[Array[Int]](0))
    }

    if(sampleType == SampleType.SIMPLE) {
      throw new InvalidParameterException("Sample with type only support type: NODE, EDGE and NODE_AND_EDGE");
    }

    val result = graph.graphMatrix.psfGet(
      new SampleNeighborWithType(new SampleNeighborWithTypeParam(graph.graphMatrix.id, nodeIds, count, sampleType, sampleMethod, NeighborStorageType.LONGARRAY)))
      .asInstanceOf[SampleNeighborResultWithType]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleNeighborsType, result.getNodeIdToSampleEdgeType)
  }

  def sampleNeighborsWithTypeWithFilter(nodeIds: Array[VertexId], filterWithNeighKeys: Array[VertexId],
                                        filterWithoutNeighKeys: Array[VertexId], count: Int = -1,
                                        sampleType: SampleType = SampleType.NODE): (
    Long2ObjectOpenHashMap[Neighbor], Long2ObjectOpenHashMap[Array[Int]], Long2ObjectOpenHashMap[Array[Int]]) = {

    if(nodeIds == null || nodeIds.length == 0) {
      // Empty param, just return
      return (new Long2ObjectOpenHashMap[Neighbor](0), new Long2ObjectOpenHashMap[Array[Int]](0), new Long2ObjectOpenHashMap[Array[Int]](0))
    }

    if(sampleType != SampleType.NODE && sampleType != SampleType.EDGE) {
      throw new InvalidParameterException("Sample with filter only support type: NODE, EDGE");
    }

    val result = graph.graphMatrix.psfGet(
      new SampleNeighborWithFilter(
        new SampleNeighborWithFilterParam(
          graph.graphMatrix.id, nodeIds, count, sampleType, filterWithNeighKeys, filterWithoutNeighKeys, NeighborStorageType.LONGARRAY)))
      .asInstanceOf[SampleNeighborResultWithType]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleNeighborsType, result.getNodeIdToSampleEdgeType)
  }

  def sampleNodeFeat(count: Int): Array[Feature] = {
    graph.graphMatrix.psfGet(new SampleNodeFeat(new SampleNodeFeatParam(graph.graphMatrix.id, count)))
      .asInstanceOf[SampleNodeFeatResult].getNodeFeats
  }

  def sampleNeighborEdgeFeat(nodeIds: Array[VertexId],
                             count: Int,
                             sampleMethod: SampleMethod = SampleMethod.RANDOM): (Long2ObjectOpenHashMap[Neighbor],
    Long2ObjectOpenHashMap[Array[Feature]]) = {
    val result = graph.graphMatrix.psfGet(new SampleEdgeFeat(
      new SampleEdgeFeatParam(graph.graphMatrix.id, nodeIds, count, sampleMethod, NeighborStorageType.LONGARRAY)))
      .asInstanceOf[SampleEdgeFeatResult]
    (result.getNodeIdToSampleNeighbors, result.getNodeIdToSampleEdgeFeats)
  }

}
