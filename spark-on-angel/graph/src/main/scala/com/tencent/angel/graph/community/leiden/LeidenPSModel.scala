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
package com.tencent.angel.graph.community.leiden

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}

private[leiden] class LeidenPSModel(val node2Community: PSVector,
                                    val old2new: PSVector,
                                    val community2Edges: PSVector,
                                    val community2Nodes: PSVector,
                                    val rank2queue: PSVector,
                                    val node2visited: PSVector
                                   ) extends Serializable {

  private val dim: Int = node2Community.dimension.toInt

  def getNode2Comm(nodes: Array[Long]): LongLongVector = {
    node2Community.pull(nodes).asInstanceOf[LongLongVector]
  }

  def getNode2Visited(nodes: Array[Long]): LongLongVector = {
    node2visited.pull(nodes).asInstanceOf[LongLongVector]
  }

  def getCommNodes(comm: Array[Long]): LongFloatVector = {
    community2Nodes.pull(comm).asInstanceOf[LongFloatVector]
  }

  def getOld2New(communities: Array[Long]): LongLongVector = {
    old2new.pull(communities).asInstanceOf[LongLongVector]
  }

  def getRankNodeStatus(nodes: Array[Long]): LongLongVector = {
    rank2queue.pull(nodes).asInstanceOf[LongLongVector]
  }

  def getCommEdges(comm: Array[Long]): LongFloatVector = {
    community2Edges.pull(comm).asInstanceOf[LongFloatVector]
  }

  def update(nodes: Array[Long], degree: Array[Float], communities: Array[Long], weights: Array[Float]): this.type = {
    updateNode2Community(nodes, communities)
    updateOld2New(nodes, communities)
    updateRank2Queue(nodes, Array.fill(nodes.length)(0))
    updateNode2Visited(nodes, Array.fill(nodes.length)(0))
    updateCommunity2Edges(communities, weights)
    updateCommunity2Nodes(communities, degree)
    this
  }

  def updateOld2New(communities1: Array[Long], communities2: Array[Long]): this.type = {
    old2new.update(VFactory.sparseLongKeyLongVector(dim, communities1, communities2))
    this
  }

  def updateNode2Community(nodes: Array[Long], communities: Array[Long]): this.type = {
    node2Community.update(VFactory.sparseLongKeyLongVector(dim, nodes, communities))
    this
  }

  def updateRank2Queue(nodes: Array[Long], statues: Array[Long]): this.type = {
    rank2queue.update(VFactory.sparseLongKeyLongVector(dim, nodes, statues))
    this
  }

  def updateNode2Visited(nodes: Array[Long], statues: Array[Long]): this.type = {
    node2visited.update(VFactory.sparseLongKeyLongVector(dim, nodes, statues))
    this
  }

  def updateCommunity2Edges(nodes: Array[Long], weights: Array[Float]): this.type = {
    community2Edges.update(VFactory.sparseLongKeyFloatVector(dim, nodes, weights))
    this
  }

  def updateCommunity2Nodes(nodes: Array[Long], degree: Array[Float]): this.type = {
    community2Nodes.update(VFactory.sparseLongKeyFloatVector(dim, nodes, degree))
    this
  }

  def getModelPart(nodes: Array[Long]): (LongLongVector, LongFloatVector, LongFloatVector) = {
    val node2community = getNode2Comm(nodes)
    val communities = node2community.getStorage.getValues.distinct
    val community2Nodes = getCommNodes(communities)
    val community2Edges = getCommEdges(communities)
    (node2community, community2Nodes, community2Edges)
  }

  def updateNode2community(nodes: Array[Long], comms: Array[Long]): this.type = {
    node2Community.update(VFactory.sparseLongKeyLongVector(dim, nodes, comms))
    this
  }

  def updateRankNodeStatus(nodes: Array[Long], status: Array[Long]): this.type = {
    rank2queue.update(VFactory.sparseLongKeyLongVector(dim, nodes, status))
    this
  }

  def resetRankNodeStatus: this.type = {
    rank2queue.reset
    this
  }

  def resetNode2Visited: this.type = {
    node2visited.reset
    this
  }

  def resetMode(): Unit = {
    Array(node2Community, old2new, community2Edges, community2Nodes, rank2queue).foreach(_.reset)
  }

  def incrementComm2nodes(comms: Array[Long], nodes: Array[Float]): this.type = {
    community2Nodes.increment(VFactory.sparseLongKeyFloatVector(dim, comms, nodes))
    this
  }

  def incrementComm2edges(comms: Array[Long], edges: Array[Float]): this.type = {
    community2Edges.increment(VFactory.sparseLongKeyFloatVector(dim, comms, edges))
    this
  }

  def updateComm2nodes(comms: Array[Long], nodes: Array[Float]): this.type = {
    community2Nodes.update(VFactory.sparseLongKeyFloatVector(dim, comms, nodes))
    this
  }

  def updateComm2edges(comms: Array[Long], edges: Array[Float]): this.type = {
    community2Edges.update(VFactory.sparseLongKeyFloatVector(dim, comms, edges))
    this
  }

}

object LeidenPSModel {
  def apply(modelContext: ModelContext): LeidenPSModel = {

    val node2commCtx = ModelContextUtils.createMatrixContext(modelContext, "node2commMatrix", RowType.T_LONG_SPARSE_LONGKEY, null)
    val node2visitedCtx = ModelContextUtils.createMatrixContext(modelContext, "node2visitedMatrix", RowType.T_LONG_SPARSE_LONGKEY, null)
    val old2newCtx = ModelContextUtils.createMatrixContext(modelContext, "old2newMatrix", RowType.T_LONG_SPARSE_LONGKEY, null)
    val node2statusCtx = ModelContextUtils.createMatrixContext(modelContext, "node2statusMatrix", RowType.T_LONG_SPARSE_LONGKEY, null)
    val community2EdgesCtx = ModelContextUtils.createMatrixContext(modelContext, "community2EdgesMatrix", RowType.T_FLOAT_SPARSE_LONGKEY, null)
    val community2NodesCtx = ModelContextUtils.createMatrixContext(modelContext, "community2NodesMatrix", RowType.T_FLOAT_SPARSE_LONGKEY, null)

    val node2commMatrix = PSMatrix.matrix(node2commCtx)
    val node2visitedMatrix = PSMatrix.matrix(node2visitedCtx)
    val old2newMatrix = PSMatrix.matrix(old2newCtx)
    val node2statusMatrix = PSMatrix.matrix(node2statusCtx)
    val community2EdgesMatrix = PSMatrix.matrix(community2EdgesCtx)
    val community2NodesMatrix = PSMatrix.matrix(community2NodesCtx)

    val node2comm = new PSVectorImpl(node2commMatrix.id, 0, modelContext.getMaxNodeId, node2commCtx.getRowType)
    val node2visited = new PSVectorImpl(node2visitedMatrix.id, 0, modelContext.getMaxNodeId, node2visitedCtx.getRowType)
    val old2new = new PSVectorImpl(old2newMatrix.id, 0, modelContext.getMaxNodeId, old2newCtx.getRowType)
    val node2status = new PSVectorImpl(node2statusMatrix.id, 0, modelContext.getMaxNodeId, node2statusCtx.getRowType)
    val community2Edges = new PSVectorImpl(community2EdgesMatrix.id, 0, modelContext.getMaxNodeId, community2EdgesCtx.getRowType)
    val community2Nodes = new PSVectorImpl(community2NodesMatrix.id, 0, modelContext.getMaxNodeId, community2NodesCtx.getRowType)

    new LeidenPSModel(node2comm, old2new, community2Edges, community2Nodes, node2status, node2visited)
  }
}