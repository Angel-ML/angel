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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.louvain

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils

import scala.collection.convert.ImplicitConversions._

class LouvainPSModel(
                      val node2CommunityPSVector: PSVector,
                      val community2weightPSVector: PSVector) extends Serializable {

  private val dim: Long = node2CommunityPSVector.dimension

  def setNode2commAndComm2weight(nodes: Array[Long], degree: Array[Float]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, nodes))
    community2weightPSVector.update(VFactory.sparseLongKeyFloatVector(dim, nodes, degree))
    this
  }

  def sumOfSquareOfCommunityWeights: Double = {
    VectorUtils.dot(community2weightPSVector, community2weightPSVector)
  }

  def sumOfCommunityWeight: Double = VectorUtils.sum(community2weightPSVector)

  def getCommInfo(comm: Array[Long]): LongFloatVector = {
    community2weightPSVector.pull(comm.clone()).asInstanceOf[LongFloatVector]
  }

  def getNode2commPairsArr(nodes: Array[Long]): Array[(Long, Long)] = {
    getNode2commMap(nodes).getStorage.asInstanceOf[LongLongSparseVectorStorage]
      .entryIterator().map { entry =>
      (entry.getLongKey, entry.getLongValue)
    }.toArray
  }

  def getNode2commMap(nodes: Array[Long]): LongLongVector = {
    node2CommunityPSVector.pull(nodes).asInstanceOf[LongLongVector]
  }

  def getCommunities(keys: Array[Long]): Array[Long] = {
    val cloneKeys = keys.clone()
    node2CommunityPSVector.pull(cloneKeys).asInstanceOf[LongLongVector].get(keys)
  }

  def getMap(keys: Array[Long]): LongLongVector = {
    node2CommunityPSVector.pull(keys).asInstanceOf[LongLongVector]
  }

  def getModelPart(nodes: Array[Long]): (LongLongVector, LongFloatVector) = {
    val node2community = node2CommunityPSVector.pull(nodes).asInstanceOf[LongLongVector]
    val communities = node2community.getStorage.asInstanceOf[LongLongSparseVectorStorage].getValues.distinct
    val community2weight = community2weightPSVector.pull(communities).asInstanceOf[LongFloatVector]
    (node2community, community2weight)
  }

  def updateNode2community(nodes: Array[Long], comms: Array[Long]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseLongKeyLongVector(dim, nodes, comms))
    this
  }

  def incrementCommWeight(comm: Array[Long], weight: Array[Float]): this.type = {
    community2weightPSVector.increment(VFactory.sparseLongKeyFloatVector(dim, comm, weight))
    this
  }

}

object LouvainPSModel {
  def apply(dim: Long): LouvainPSModel = {
    val id2comm = PSVector.sparse(dim, 1, rowType = RowType.T_LONG_SPARSE_LONGKEY)
    val comm2weight = PSVector.dense(dim, 1, rowType = RowType.T_FLOAT_SPARSE_LONGKEY)
    new LouvainPSModel(id2comm, comm2weight)
  }
}
