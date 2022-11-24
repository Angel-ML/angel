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

package com.tencent.angel.graph.rank.closeness

import java.lang.{Double => JDouble, Long => JLong}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage
import com.tencent.angel.ml.math2.vector.IntLongVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult
import com.tencent.angel.graph.psf.hyperanf._
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2ObjectOpenHashMap}
import org.apache.spark.rdd.RDD

private[closeness]
class ClosenessPSModel(matrix: PSMatrix) extends Serializable {

  final val matrixId = matrix.id
  final val dim = matrix.columns

  def checkpoint(): Unit = {
    matrix.checkpoint()
  }

  def init(nodes: Array[Long], p: Int, sp: Int, seed: Long): Unit = {
    val func = new InitHyperLogLog(matrix.id, p, sp, nodes, seed)
    matrix.psfUpdate(func).get()
  }

  def getHyperLogLog(nodes: Array[Long]): Long2ObjectOpenHashMap[HyperLogLogPlus] = {
    val func = new GetHyperLogLog(matrix.id, nodes)
    matrix.psfGet(func).asInstanceOf[GetHyperLogLogResult].getResults
  }

  def sendMsgs(updates: Long2ObjectOpenHashMap[HyperLogLogPlus], p: Int, sp: Int, seed: Long): Unit = {
    val func = new UpdateHyperLogLog(matrix.id, updates, p, sp, seed)
    matrix.psfUpdate(func).get()
  }

  def computeCloseness(r: Int, isConnected: Boolean): Unit = {
    val func = new ComputeCloseness(matrix.id, r, isConnected)
    matrix.psfUpdate(func).get()
  }

  def getNodes(partitionIds: Array[Int]): Array[Long] = {
    val func = new GetNodes(matrix.id, partitionIds)
    matrix.psfGet(func).asInstanceOf[GetRowResult].getRow.asInstanceOf[IntLongVector]
      .getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
  }

  def readCloseness(nodes: Array[Long], numNodes: Long, isConnected: Boolean): Long2DoubleOpenHashMap = {
    val func = new GetCloseness(matrix.id, nodes, numNodes, isConnected)
    matrix.psfGet(func).asInstanceOf[GetClosenessResult].getResults
  }

  def readClosenessAndCardinality(nodes: Array[Long],
                                  numNodes: Long,
                                  isDirected: Boolean,
                                  isConnected: Boolean): Long2ObjectOpenHashMap[(JDouble, JLong, JDouble)] = {
    val func = new GetClosenessAndCardinality(matrix.id, nodes, numNodes, isDirected, isConnected)
    matrix.psfGet(func).asInstanceOf[GetClosenessAndCardinalityResult].getResults
  }

  def maxCardinality(): Long = {
    val func = new MaxCardinality(matrix.id, 0)
    matrix.psfGet(func).asInstanceOf[ScalarAggrResult].getResult.toLong
  }

  def numNodes(): Long = {
    val func = new NumNodes(matrix.id)
    matrix.psfGet(func).asInstanceOf[NumNodesResult].getResult
  }

}

object ClosenessPSModel {
  def apply(modelContext: ModelContext,
            index: RDD[Long],
            useBalancePartition: Boolean,
            percent: Float): ClosenessPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[HyperLogLogPlusElement])

    if (useBalancePartition)
      LoadBalancePartitioner.partition(index, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, percent)

    val psMatrix = PSMatrix.matrix(matrix)
    new ClosenessPSModel(new PSMatrixImpl(psMatrix.id, matrix.getName, 1, modelContext.getMaxNodeId, matrix.getRowType))
  }
}
