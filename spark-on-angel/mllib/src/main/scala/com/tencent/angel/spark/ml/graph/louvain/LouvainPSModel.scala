package com.tencent.angel.spark.ml.graph.louvain

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntFloatVector, IntIntVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.PSVector

class LouvainPSModel(
    val node2CommunityPSVector: PSVector,
    val community2weightPSVector: PSVector) extends Serializable {

  private val dim: Int = node2CommunityPSVector.dimension.toInt

  def initialize(nodes: Array[Int], degree: Array[Float]): this.type = {
    node2CommunityPSVector.update(VFactory.sparseIntVector(dim, nodes, nodes))
    community2weightPSVector.update(VFactory.sparseFloatVector(dim, nodes, degree))
    this
  }

  def fetch(
      start: Int,
      end: Int,
      keys: Array[Int],
      adjs: Array[Array[Int]]): (IntIntVector, IntFloatVector) = {

    val nodes = (adjs.slice(start, end).flatten ++ keys.slice(start, end)).distinct
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
