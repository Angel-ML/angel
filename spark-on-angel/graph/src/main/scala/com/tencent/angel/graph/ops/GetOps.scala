/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.ops

import com.tencent.angel.graph.client.psf.get.getedgefeats.{GetEdgeFeats, GetEdgeFeatsResult}
import com.tencent.angel.graph.client.psf.get.getedgetypes.GetEdgeTypes
import com.tencent.angel.graph.client.psf.get.getedgeweights.GetEdgeWeights
import com.tencent.angel.graph.client.psf.get.getlabels.GetLabels
import com.tencent.angel.graph.client.psf.get.getneighbors.{GetNeighbors, GetNeighborsResult}
import com.tencent.angel.graph.client.psf.get.getnodefeats.{GetNodeFeats, GetNodeFeatsResult}
import com.tencent.angel.graph.client.psf.get.getnodetypes.GetNodeTypes
import com.tencent.angel.graph.client.psf.get.utils.{GetNodeAttrsParam, GetFloatArrayAttrsResult, GetIntArrayAttrsResult}
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.graph.GraphModel
import com.tencent.angel.graph.data.VertexId
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class GetOps(graph: GraphModel) extends Serializable {

  def getNodeFeats(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[IntFloatVector] = {
    graph.graphMatrix.psfGet(new GetNodeFeats(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetNodeFeatsResult].getnodeIdToFeats
  }

  def getEdgeFeats(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[IntFloatVector]] = {
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

  def getNeighbors(nodeIds: Array[VertexId]): Long2ObjectOpenHashMap[Array[Long]] = {
    graph.graphMatrix.psfGet(new GetNeighbors(new GetNodeAttrsParam(graph.graphMatrix.id, nodeIds)))
      .asInstanceOf[GetNeighborsResult].getNodeIdToContents
  }

}
