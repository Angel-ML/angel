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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntLongVector, LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.pagerank.GetNodes
import com.tencent.angel.spark.ml.psf.sssp.{SSSPMinFunc, SSSPMinParam}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class SSSPPSModel(var inMsgs: PSVector,
                  var outMsgs: PSVector,
                  distance: PSVector) extends Serializable {
  val dim = inMsgs.dimension
  val matrixId = inMsgs.poolId

  def readDistances(nodes: Array[Long]): LongFloatVector =
    distance.pull(nodes).asInstanceOf[LongFloatVector]

  def readMsgs(nodes: Array[Long]): LongFloatVector =
    inMsgs.pull(nodes).asInstanceOf[LongFloatVector]

  def readAllMsgs(): LongFloatVector =
    inMsgs.pull().asInstanceOf[LongFloatVector]

  def updateMsgs(msgs: Vector): Unit = {
    val param = new SSSPMinParam(outMsgs.poolId, Array(msgs),
      Array(outMsgs.id, distance.id))
    val func = new SSSPMinFunc(param)
    distance.psfUpdate(func).get()
  }

  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)

  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }

  def getNodes(partitionIds: Array[Int]): Array[Long] = {
    val func = new GetNodes(distance.poolId, partitionIds)
    distance.psfGet(func).asInstanceOf[GetRowResult].getRow.asInstanceOf[IntLongVector]
      .getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
  }
}


private[sssp] object SSSPPSModel {
  def fromMinMax(minId: Long, maxId: Long, index: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): SSSPPSModel = {
    val matrix = new MatrixContext("sssp", 3, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY)
    matrix.setPartitionerClass(classOf[ColumnRangePartitioner])

    // use balance partition
    if (useBalancePartition)
      LoadBalancePartitioner.partition(index, maxId, psNumPartition, matrix)

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("sssp").getId
    new SSSPPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 2, maxId, matrix.getRowType))
  }
}