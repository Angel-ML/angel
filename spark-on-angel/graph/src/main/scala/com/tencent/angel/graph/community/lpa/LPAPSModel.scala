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
package com.tencent.angel.graph.community.lpa

import com.tencent.angel.ml.math2.vector.{LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD

class LPAPSModel(var inMsgs: PSVector,
                 var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension

  /**
    * init nodes label
    *
    * @param msgs init  < node,labelId > key-value vector from ps
    */
  def initMsgs(msgs: Vector): Unit =
    inMsgs.update(msgs)

  /**
    * read nodes label
    *
    * @param nodes read < node,labelId > key-value vector from ps
    * @return
    */
  def readMsgs(nodes: Array[Long]): LongLongVector =
    inMsgs.pull(nodes).asInstanceOf[LongLongVector]

  /**
    * write nodes label
    *
    * @param msgs write < node,labelId > key-value vector from ps
    */
  def writeMsgs(msgs: Vector): Unit =
    outMsgs.update(msgs)

  /**
    * two PSVector exchange data
    */
  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }
}

object LPAPSModel {

  /**
    * to balance < node, labelId > key-value vector on ps
    *
    * @param minId                   minId in nodes
    * @param maxId                   maxId in nodes
    * @param data                    nodes
    * @param psNumPartition          ps-partition num
    * @param useBalancePartition     to balance ps-partition region
    * @param balancePartitionPercent the max partition cannot  store more than 70%  < node,labelId>  key-value
    * @return
    */
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean, balancePartitionPercent: Float = 0.7f): LPAPSModel = {
    val matrix = new MatrixContext("lpa", 2, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
    // use balance partition
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)
    }

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("lpa").getId
    new LPAPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType))
  }
}
