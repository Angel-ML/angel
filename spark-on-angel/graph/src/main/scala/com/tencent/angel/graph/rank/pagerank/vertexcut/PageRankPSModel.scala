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

package com.tencent.angel.graph.rank.pagerank.vertexcut

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.ml.math2.vector.{LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.graph.rank.pagerank.PageRankModel
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.spark.ml.util.{LoadBalancePartitioner, LoadBalanceWithEstimatePartitioner}
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[vertexcut]
class PageRankPSModel(readMsgs: PSVector,
                      writeMsgs: PSVector,
                      ranks: PSVector,
                      sums: PSVector) extends PageRankModel(readMsgs, writeMsgs, ranks) {

  def updateSums(update: Vector): Unit = {
    sums.increment(update)
  }

  def readSums(keys: Array[Long]): LongFloatVector =
    sums.pull(keys).asInstanceOf[LongFloatVector]


}

private[vertexcut] object PageRankPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
                 useBalancePartition: Boolean, useEstimatePartitioner: Boolean,
                 balancePartitionPercent: Float): PageRankPSModel = {

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

    new PageRankPSModel(new PSVectorImpl(matrixId, 0, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 2, matrixMaxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 3, matrixMaxId, matrix.getRowType))
  }

}

