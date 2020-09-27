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

package com.tencent.angel.graph.embedding.deepwalk

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.initaliastable.{InitNeighborAliasTable, InitNeighborAliasTableParam}
import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.samplealiastable.{GetNeighborAliasTable, GetNeighborAliasTableParam, GetNeighborAliasTableResult, NeighborsAliasTableElement}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class DeepWalkPSModel (var edgesPsMatrix: PSMatrix) extends Serializable{
  //push node adjacency list
  def initNodeNei(msgs: Long2ObjectOpenHashMap[NeighborsAliasTableElement]): Unit = {
    val func = new InitNeighborAliasTable(new InitNeighborAliasTableParam(edgesPsMatrix.id, msgs))
    edgesPsMatrix.asyncPsfUpdate(func).get()
  }

  //pull node adjacency list
  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int]): Long2ObjectOpenHashMap[Array[Long]] = {
    psMatrix.psfGet(new GetNeighborAliasTable(new GetNeighborAliasTableParam(psMatrix.id, nodeIds, count)))
      .asInstanceOf[GetNeighborAliasTableResult].getNodeIdToNeighbors
  }

}

object DeepWalkPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): DeepWalkPSModel = {
    val edgesMatrix = new MatrixContext()
    edgesMatrix.setName("edgeMatrix")
    edgesMatrix.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    edgesMatrix.setRowNum(1)
    edgesMatrix.setColNum(maxId - minId)
    edgesMatrix.setMaxColNumInBlock(maxId / psNumPartition)
    edgesMatrix.setValueType(classOf[NeighborsAliasTableElement])
    edgesMatrix.setIndexEnd(maxId)
    edgesMatrix.setIndexStart(minId)

    // use balance ps partition
    if (useBalancePartition) {
      //LoadBalancePartitioner.partition(data, maxId, psNumPartition, edgesMatrix)
      LoadBalancePartitioner.partition(data, psNumPartition, edgesMatrix)
    }

    val edgesPsMatrix = PSMatrix.matrix(edgesMatrix)
    new DeepWalkPSModel(edgesPsMatrix)
  }
}
