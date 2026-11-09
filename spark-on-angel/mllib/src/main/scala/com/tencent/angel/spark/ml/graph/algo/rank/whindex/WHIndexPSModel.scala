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
package com.tencent.angel.spark.ml.graph.algo.rank.whindex

import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD


class WHIndexPSModel(var degree: PSVector,
                     var wh: PSVector) extends Serializable {
  val dim: Long = degree.dimension

  def initDegree(msgs: Vector): Unit =
    degree.update(msgs)

  def readDegree(nodes: Array[Long]): LongIntVector =
    degree.pull(nodes).asInstanceOf[LongIntVector]

  def readAllDegree(): LongIntVector =
    degree.pull().asInstanceOf[LongIntVector]

  def updatewh(msgs: Vector): Unit = {
    wh.update(msgs)
  }

  def readwh(nodes: Array[Long]): LongIntVector =
    wh.pull(nodes).asInstanceOf[LongIntVector]

}

object WHIndexPSModel {


  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): WHIndexPSModel = {
    val matrix = new MatrixContext("whindex", 2, minId, maxId)
    matrix.setValidIndexNum(-1)

    //set rowType T_Int_Sparse_LongKey
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    // use balance partition
    if (useBalancePartition)
      LoadBalancePartitioner.partition(data, psNumPartition, matrix)

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("whindex").getId
    new WHIndexPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType))
  }
}
