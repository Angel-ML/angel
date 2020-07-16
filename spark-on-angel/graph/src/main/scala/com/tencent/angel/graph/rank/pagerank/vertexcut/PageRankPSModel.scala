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

import com.tencent.angel.ml.math2.vector.{LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.rank.pagerank.PageRankModel
import com.tencent.angel.graph.psf.pagerank._
import com.tencent.angel.spark.ml.util.{LoadBalancePartitioner, LoadBalanceWithEstimatePartitioner}
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD

private[vertexcut]
class PageRankPSModel(readMsgs: PSVector,
                      writeMsgs: PSVector,
                      ranks: PSVector,
                      sums: PSVector) extends PageRankModel(readMsgs, writeMsgs, ranks) {

  def updateSums(update: Vector): Unit = {
    update.setRowId(sums.id)
    sums.psfUpdate(new MyIncrement(new IncrementRowsParam(sums.poolId, Array(update)))).get()
  }

  def readSums(keys: Array[Long]): LongFloatVector =
    sums.pull(keys).asInstanceOf[LongFloatVector]


}

private[vertexcut] object PageRankPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
                 useBalancePartition: Boolean, useEstimatePartitioner: Boolean,
                 balancePartitionPercent: Float = 0.7f): PageRankPSModel = {
    val matrix = new MatrixContext("pagerank", 4, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY)
    matrix.setPartitionerClass(classOf[ColumnRangePartitioner])

    // If useEstimatePartitioner is true, means use LoadBalanceWithEstimatePartitioner
    // If useEstimatePartitioner is false and useBalancePartition is true, means use LoadBalancePartitioner
    if (useEstimatePartitioner) {
      LoadBalanceWithEstimatePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)
    } else if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)
    }

    PSContext.instance().createMatrix(matrix)
    //PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("pagerank").getId
    new PageRankPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 2, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 3, maxId, matrix.getRowType))
  }

}

