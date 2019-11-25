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
package com.tencent.angel.spark.ml.graph.kcore2

import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD

private[kcore2] class KCorePSModel(val core: PSVector) extends Serializable {

  val dim: Int = core.dimension.toInt

  def update(update: Vector): Unit =
    core.update(update)

  def pull(nodes: Array[Long]): LongIntVector =
    core.pull(nodes).asInstanceOf[LongIntVector]

}

private[kcore2] object KCorePSModel {

  def fromMaxId(maxId: Long, data: RDD[Long], psPartitionNum: Int): KCorePSModel = {
    val matrix = new MatrixContext("cores", 1, 0, maxId)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    matrix.setValidIndexNum(-1)
    val partitioner = new LoadBalancePartitioner(40, psPartitionNum)
    partitioner.partitionIndex(data, matrix)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("cores").getId
    new KCorePSModel(new PSVectorImpl(matrixId, 0, maxId, rowType = matrix.getRowType))

  }

  def fromMaxMin(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int): KCorePSModel = {
    val matrix = new MatrixContext("cores", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    val bits = (numBits(maxId) * 0.7).toInt
    val partitioner = new LoadBalancePartitioner(bits, psNumPartition)
    partitioner.partitionIndex(data, matrix)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("cores").getId
    new KCorePSModel(new PSVectorImpl(matrixId, 0, maxId, rowType = matrix.getRowType))
  }

  def numBits(maxId: Long): Int = {
    var num = 0
    var value = maxId
    while (value > 0) {
      value >>= 1
      num += 1
    }
    num
  }
}
