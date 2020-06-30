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
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntFloatVector, IntIntVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils

import scala.collection.JavaConversions._

class LouvainPSModel(
                      val node2CommunityPSVector: PSVector,
                      val community2weightPSVector: PSVector) extends Serializable {

  private val dim: Int = node2CommunityPSVector.dimension.toInt

  def setNode2commAndComm2weight(nodes: Array[Int], degree: Array[Float]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseIntVector(dim, nodes, nodes))
    community2weightPSVector.update(VFactory.sparseFloatVector(dim, nodes, degree))
    this
  }

  def sumOfSquareOfCommunityWeights: Double = {
    VectorUtils.dot(community2weightPSVector, community2weightPSVector)
  }

  def sumOfCommunityWeight: Double = VectorUtils.sum(community2weightPSVector)

  def getCommInfo(comm: Array[Int]): IntFloatVector = {
    community2weightPSVector.pull(comm.clone()).asInstanceOf[IntFloatVector]
  }

  def getNode2commPairsArr(nodes: Array[Int]): Array[(Int, Int)] = {
    getNode2commMap(nodes).getStorage.asInstanceOf[IntIntSparseVectorStorage]
      .entryIterator().map { entry =>
      (entry.getIntKey, entry.getIntValue)
    }.toArray
  }

  def getNode2commMap(nodes: Array[Int]): IntIntVector = {
    node2CommunityPSVector.pull(nodes).asInstanceOf[IntIntVector]
  }

  def getCommunities(keys: Array[Int]): Array[Int] = {
    val cloneKeys = keys.clone()
    node2CommunityPSVector.pull(cloneKeys).asInstanceOf[IntIntVector].get(keys)
  }

  def getMap(keys: Array[Int]): IntIntVector = {
    node2CommunityPSVector.pull(keys).asInstanceOf[IntIntVector]
  }


  def getModelPart(nodes: Array[Int]): (IntIntVector, IntFloatVector) = {
    val node2community = node2CommunityPSVector.pull(nodes).asInstanceOf[IntIntVector]
    val communities = node2community.getStorage.asInstanceOf[IntIntSparseVectorStorage].getValues.distinct
    val community2weight = community2weightPSVector.pull(communities).asInstanceOf[IntFloatVector]
    (node2community, community2weight)
  }

  def updateNode2community(nodes: Array[Int], comms: Array[Int]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseIntVector(dim, nodes, comms))
    this
  }

  def incrementCommWeight(comm: Array[Int], weight: Array[Float]): this.type = {
    community2weightPSVector.increment(VFactory.sparseFloatVector(dim, comm, weight))
    this
  }

}

object LouvainPSModel {
  def apply(dim: Int): LouvainPSModel = {
    val id2comm = PSVector.dense(dim, 1, rowType = RowType.T_INT_DENSE)
    val comm2weight = PSVector.dense(dim, 1, rowType = RowType.T_FLOAT_DENSE)
    new LouvainPSModel(id2comm, comm2weight)
  }
}
