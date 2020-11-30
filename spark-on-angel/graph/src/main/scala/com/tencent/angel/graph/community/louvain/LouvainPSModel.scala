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
package com.tencent.angel.graph.community.louvain

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils

import scala.collection.JavaConversions._

class LouvainPSModel(
                      val node2CommunityPSVector: PSVector,
                      val community2weightPSVector: PSVector,
                      val node2CommunityFianlPSVector: PSVector) extends Serializable {

  private val dim: Long = node2CommunityPSVector.dimension

  /**
   * set node community with node self id;set the community weight
   *
   * @param nodes  the nodes list
   * @param degree the degree list
   * @return
   */
  def setNode2commAndComm2weight(nodes: Array[Long], degree: Array[Float]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))
    node2CommunityFianlPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))
    community2weightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, degree))
    this
  }


  def updateNodeCommunityFinalPSFunction(nodes: Array[Long]): this.type = {
    val oldCommunity = node2CommunityFianlPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector].get(nodes)
    val newCommunity = node2CommunityPSVector.pull(oldCommunity.clone()).asInstanceOf[LongLongVector].get(oldCommunity)
    node2CommunityFianlPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes.clone(), newCommunity))
    this
  }


  /**
   * get the sum of square (all community weights)
   *
   * @return
   */
  def sumOfSquareOfCommunityWeights: Double = {
    VectorUtils.dot(community2weightPSVector, community2weightPSVector)
  }

  /**
   * get the sum of all community weights
   *
   * @return
   */
  def sumOfCommunityWeight: Double = VectorUtils.sum(community2weightPSVector)

  /**
   * get the community weights
   *
   * @param comm the community list
   * @return
   */
  def getCommInfo(comm: Array[Long]): LongFloatVector = {
    community2weightPSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]
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
    node2CommunityPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
  }

  /**
   * get the community of nodes in array
   *
   * @param keys the node list
   * @return
   */
  def getCommunities(keys: Array[Long]): Array[Long] = {
    node2CommunityPSVector.pull(keys.clone()).asInstanceOf[LongLongVector].get(keys)
  }

  /**
   * get the community of nodes in map
   *
   * @param keys the node list
   * @return
   */
  def getMap(keys: Array[Long]): LongLongVector = {
    node2CommunityPSVector.pull(keys.clone()).asInstanceOf[LongLongVector]
  }

  /**
   * get the community of nodes and community weight
   *
   * @param nodes the node list
   * @return
   */
  def getModelPart(nodes: Array[Long]): (LongLongVector, LongFloatVector) = {
    val node2community = node2CommunityPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector]
    val communities = node2community.getStorage.asInstanceOf[LongLongSparseVectorStorage].getValues.distinct
    val community2weight = community2weightPSVector.pull(communities.clone()).asInstanceOf[LongFloatVector]
    (node2community, community2weight)
  }

  /**
   * update the community of nodes
   *
   * @param nodes the nodes list
   * @param comms the communities list
   * @return
   */
  def updateNode2community(nodes: Array[Long], comms: Array[Long]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, comms))
    this
  }

  /**
   * increase the community weight
   *
   * @param comm   the communities list
   * @param weight the delta value
   * @return
   */
  def incrementCommWeight(comm: Array[Long], weight: Array[Float]): this.type = {
    community2weightPSVector.increment(VFactory.sparseLongKeyFloatVector(dim, comm, weight))
    this
  }

}

object LouvainPSModel {
  /**
   * generate the louvain PS model;
   * the ps vector of node to community
   * the ps vector of community to corresponding weight
   *
   * @param dim the ps vector dimensionality
   * @return
   */
  def apply(dim: Long): LouvainPSModel = {
    val id2comm = PSVector.sparse(dim, 1, rowType = RowType.T_LONG_SPARSE_LONGKEY)
    val id2commFianl = PSVector.sparse(dim, 1, rowType = RowType.T_LONG_SPARSE_LONGKEY)
    val comm2weight = PSVector.dense(dim, 1, rowType = RowType.T_FLOAT_SPARSE_LONGKEY)
    new LouvainPSModel(id2comm, comm2weight, id2commFianl)
  }
}
