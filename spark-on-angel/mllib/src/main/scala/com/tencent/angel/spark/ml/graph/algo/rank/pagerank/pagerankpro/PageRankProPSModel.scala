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
package com.tencent.angel.spark.ml.graph.algo.rank.pagerank.pagerankpro

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.ml.graph.algo.rank.pagerank.PageRankModel
import com.tencent.angel.spark.ml.psf.pagerank.{ComputeRank, ComputeRank3}
import com.tencent.angel.spark.ml.util.{LoadBalancePartitioner, LoadBalanceWithEstimatePartitioner}
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PageRankProPSModel(readMsgs: PSVector,
                         writeMsgs: PSVector,
                         ranks: PSVector,
                         initRanks: PSVector) extends PageRankModel(readMsgs, writeMsgs, ranks) {

  def minRank() = {
    VectorUtils.min(ranks)
  }

  def numWriteMsgs(): Long = VectorUtils.nnz(writeMsgs)

  def initLabelRank(values: Vector): Unit = {
    initRanks.update(values)
    ranks.update(values)
  }

  def initLabelRank(initRank: RDD[(Long, Float)]): Unit = {
    initRank.mapPartitions{ iter =>
      val msgs = VFactory.sparseLongKeyFloatVector(this.dim)
      while (iter.hasNext) {
        val (n, r) = iter.next()
        msgs.set(n, r)
      }
      initLabelRank(msgs)
      Iterator.single(msgs.size())
    }.count()
  }

  def computeRanks1(initRanks: Float, resetProb: Float): Unit = {
    val func = new ComputeRank(readMsgs.poolId,
      Array(readMsgs.id, writeMsgs.id, ranks.id),
      initRanks, resetProb, 1)

    readMsgs.psfUpdate(func).get()
  }

  def computeRanks3(resetProb: Float, resizeFlag: Int=1): Unit = {
    val func = new ComputeRank3(readMsgs.poolId,
      Array(readMsgs.id, writeMsgs.id, ranks.id, initRanks.id),
      0f, resetProb, resizeFlag)

    readMsgs.psfUpdate(func).get()
  }

  def rankSum() = VectorUtils.sum(ranks)

  def initRankSum() = VectorUtils.sum(initRanks)

}

object PageRankProPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
                 useBalancePartition: Boolean, useEstimatePartitioner: Boolean,
                 balancePartitionPercent: Float): PageRankProPSModel = {

    val nodesNum = data.countApproxDistinct()

    val modelContext = new ModelContext(
      psNumPartition, minId, maxId + 1, nodesNum, "pagerank", SparkContext.getOrCreate().hadoopConfiguration)

    // Create a matrix for embedding vectors
    val matrixMaxId = modelContext.getMaxNodeId
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_FLOAT_SPARSE_LONGKEY, 4)

    // load balance for range partition
    if (!modelContext.isUseHashPartition) {
      if (useEstimatePartitioner) {
        LoadBalanceWithEstimatePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)
      } else if (useBalancePartition) {
        LoadBalancePartitioner.partition(data, psNumPartition, matrix)
      }
    }

    //create ps matrix
    val psMatrix = PSMatrix.matrix(matrix)
    val matrixId = psMatrix.id

    new PageRankProPSModel(new PSVectorImpl(matrixId, 0, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 2, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 3, matrixMaxId, matrix.getRowType))
  }
}
