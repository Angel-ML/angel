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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainDensity

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import org.apache.spark.rdd.RDD

import scala.collection.convert.ImplicitConversions._

class LouvainDensityPSModel(
                             var node2CommunityInPSVector: PSVector,
                             var node2CommunityOutPSVector: PSVector,
                             val node2CommunityFinalPSVector: PSVector,
                             val community2weightPSVector: PSVector,
                             val comm2NumberSelfEdgePSVector: PSVector,
                             val comm2SizePSVector: PSVector,
                             val comm2SelfWeightPSVector: PSVector) extends Serializable {

  private val dim: Long = node2CommunityInPSVector.dimension

  /**
   * set node community with node self id;set the community weight
   *
   * @param nodes  the nodes list
   * @param degree the degree list
   * @return
   */
  def setNode2commAndComm2weight(nodes: Array[Long], degree: Array[Float]): this.type = {
    val length = nodes.length
    node2CommunityInPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))
    node2CommunityOutPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))
    node2CommunityFinalPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))

    community2weightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, degree))
    comm2NumberSelfEdgePSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, Array.fill(length)(0f)))
    comm2SizePSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, Array.fill(length)(1f)))
    comm2SelfWeightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, Array.fill(length)(0f)))
    this
  }


  def updateNodeCommunityFinalPSFunction(nodes: Array[Long]): this.type = {
    val oldCommunity = node2CommunityFinalPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector].get(nodes)
    val newCommunity = node2CommunityInPSVector.pull(oldCommunity.clone()).asInstanceOf[LongLongVector].get(oldCommunity)
    node2CommunityFinalPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes.clone(), newCommunity))
    this
  }


  def getCommAllInfor(comm: Array[Long]): (LongFloatVector, LongFloatVector, LongFloatVector, LongFloatVector) = {
    val community2weight = community2weightPSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]
    val comm2NumberSelfEdge = comm2NumberSelfEdgePSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]
    val comm2Size = comm2SizePSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]
    val comm2SelfWeight = comm2SelfWeightPSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]

    //println("community2weight")
    //println(community2weight.get(comm).toArray.mkString(","))
    (community2weight, comm2NumberSelfEdge, comm2Size, comm2SelfWeight)
  }

  /**
   * get the community of nodes in array pair
   *
   * @param nodes the node list
   * @return
   */
  def getNode2commPairsArr(nodes: Array[Long]): Array[(Long, Long)] = {
    getNode2commMap(nodes).getStorage.asInstanceOf[LongLongSparseVectorStorage]
      .entryIterator().map { entry =>
      (entry.getLongKey, entry.getLongValue)
    }.toArray
  }

  /**
   * get the community of nodes in map
   *
   * @param nodes the node list
   * @return
   */
  def getNode2commMap(nodes: Array[Long]): LongLongVector = {
    node2CommunityInPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
  }

  /**
   * get the community of nodes in map
   *
   * @param nodes the node list
   * @return
   */
  def getNode2commMapFromOutVector(nodes: Array[Long]): LongLongVector = {
    node2CommunityOutPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
  }

  /**
   * get the community of nodes in map
   *
   * @param nodes the node list
   * @return
   */
  def getNode2commMapFromBothVector(nodes: Array[Long], source: String): LongLongVector = {
    if (source == "out") {
      node2CommunityOutPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
    }
    else
      node2CommunityInPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
  }


  /**
   * get the community of nodes in array
   *
   * @param keys the node list
   * @return
   */
  def getCommunitiesFromInVector(keys: Array[Long]): Array[Long] = {
    node2CommunityInPSVector.pull(keys.clone()).asInstanceOf[LongLongVector].get(keys)
  }

  /**
   * get the community of nodes in array
   *
   * @param keys the node list
   * @return
   */
  def getCommunitiesFromOutVector(keys: Array[Long]): Array[Long] = {
    node2CommunityOutPSVector.pull(keys.clone()).asInstanceOf[LongLongVector].get(keys)
  }

  /**
   * get the community of nodes and community weight
   *
   * @param nodes the node list
   * @return
   */
  def getModelPart(nodes: Array[Long]): (LongLongVector, LongFloatVector, LongFloatVector, LongFloatVector, LongFloatVector) = {
    val node2community = node2CommunityInPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]

    val communities = node2community.getStorage.asInstanceOf[LongLongSparseVectorStorage].getValues.distinct
    val community2weight = community2weightPSVector.pull(communities.clone()).asInstanceOf[LongFloatVector]
    val comm2NumberSelfEdge = comm2NumberSelfEdgePSVector.pull(communities.clone()).asInstanceOf[LongFloatVector]
    val comm2Size = comm2SizePSVector.pull(communities.clone()).asInstanceOf[LongFloatVector]
    val comm2SelfWeight = comm2SelfWeightPSVector.pull(communities.clone()).asInstanceOf[LongFloatVector]

    (node2community, community2weight, comm2NumberSelfEdge, comm2Size, comm2SelfWeight)
  }

  /**
   * update the community of nodes
   *
   * @param nodes the nodes list
   * @param comms the communities list
   * @return
   */
  def updateNode2communityOut(nodes: Array[Long], comms: Array[Long]): this.type = {
    node2CommunityOutPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, comms))
    this
  }


  def updateNode2communityIn(nodes: Array[Long], comms: Array[Long]): this.type = {
    node2CommunityInPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, comms))
    this
  }

  def updateCommEdgesInfor(comm: Array[Long], selfWeigh: Array[Float], numberSelfEdge: Array[Float]): this.type = {
    comm2SelfWeightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, comm, selfWeigh))
    comm2NumberSelfEdgePSVector.update(VFactory.sparseLongKeyFloatVector(dim, comm, numberSelfEdge))
    this
  }

  def updateCommNodesInfor(comm: Array[Long], weight: Array[Float], size: Array[Float]): this.type = {
    community2weightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, comm, weight))
    comm2SizePSVector.update(VFactory.sparseLongKeyFloatVector(dim, comm, size))
    this
  }

  def resetMsgs(): Unit = {
    val temp = node2CommunityInPSVector
    node2CommunityInPSVector = node2CommunityOutPSVector
    node2CommunityOutPSVector = temp
  }
}


object LouvainDensityPSModel {
  /**
   * generate the louvain PS model;
   * the ps vector of node to community
   * the ps vector of community to corresponding weight
   *
   * @return
   */
  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float): LouvainDensityPSModel = {

    val id2commInMatrix = ModelContextUtils.createMatrixContext(modelContext, "id2commIn", RowType.T_LONG_SPARSE_LONGKEY, null)
    val id2commOutMatrix = ModelContextUtils.createMatrixContext(modelContext, "id2commOut", RowType.T_LONG_SPARSE_LONGKEY, null)
    val id2commFinalMatrix = ModelContextUtils.createMatrixContext(modelContext, "id2commFinal", RowType.T_LONG_SPARSE_LONGKEY, null)
    val comm2WeightMatrix = ModelContextUtils.createMatrixContext(modelContext, "comm2Weight", RowType.T_FLOAT_SPARSE_LONGKEY, null)
    val comm2NumberSelfEdgeMatrix = ModelContextUtils.createMatrixContext(modelContext, "comm2NumberSelfEdge", RowType.T_FLOAT_SPARSE_LONGKEY, null)
    val comm2SizeMatrix = ModelContextUtils.createMatrixContext(modelContext, "comm2Size", RowType.T_FLOAT_SPARSE_LONGKEY, null)
    val comm2SelfWeightMatrix = ModelContextUtils.createMatrixContext(modelContext, "comm2SelfWeight", RowType.T_FLOAT_SPARSE_LONGKEY, null)

    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      LoadBalancePartitioner.partition(data, modelContext.getMaxNodeId, modelContext.getPartitionNum, id2commInMatrix, balancePartitionPercent)
      val PartsNode = id2commInMatrix.getParts

      id2commOutMatrix.setParts(PartsNode)
      id2commFinalMatrix.setParts(PartsNode)
      comm2WeightMatrix.setParts(PartsNode)
      comm2NumberSelfEdgeMatrix.setParts(PartsNode)
      comm2SizeMatrix.setParts(PartsNode)
      comm2SelfWeightMatrix.setParts(PartsNode)
    }


    val id2commInPsMatrix = PSMatrix.matrix(id2commInMatrix)
    val id2commOutPsMatrix = PSMatrix.matrix(id2commOutMatrix)
    val id2commFinalPsMatrix = PSMatrix.matrix(id2commFinalMatrix)
    val comm2WeightPsMatrix = PSMatrix.matrix(comm2WeightMatrix)
    val comm2NumberSelfEdgePsMatrix = PSMatrix.matrix(comm2NumberSelfEdgeMatrix)
    val comm2SizePsMatrix = PSMatrix.matrix(comm2SizeMatrix)
    val comm2SelfWeightPsMatrix = PSMatrix.matrix(comm2SelfWeightMatrix)


    val id2commIn = new PSVectorImpl(id2commInPsMatrix.id, 0, modelContext.getMaxNodeId, id2commInMatrix.getRowType)
    val id2commOut = new PSVectorImpl(id2commOutPsMatrix.id, 0, modelContext.getMaxNodeId, id2commOutMatrix.getRowType)
    val id2commFinal = new PSVectorImpl(id2commFinalPsMatrix.id, 0, modelContext.getMaxNodeId, id2commFinalMatrix.getRowType)
    val comm2Weight = new PSVectorImpl(comm2WeightPsMatrix.id, 0, modelContext.getMaxNodeId, comm2WeightMatrix.getRowType)
    val comm2NumberSelfEdge = new PSVectorImpl(comm2NumberSelfEdgePsMatrix.id, 0, modelContext.getMaxNodeId, comm2NumberSelfEdgeMatrix.getRowType)
    val comm2Size = new PSVectorImpl(comm2SizePsMatrix.id, 0, modelContext.getMaxNodeId, comm2SizeMatrix.getRowType)
    val comm2SelfWeight = new PSVectorImpl(comm2SelfWeightPsMatrix.id, 0, modelContext.getMaxNodeId, comm2SelfWeightMatrix.getRowType)

    new LouvainDensityPSModel(id2commIn, id2commOut, id2commFinal, comm2Weight, comm2NumberSelfEdge, comm2Size, comm2SelfWeight)
  }
}
