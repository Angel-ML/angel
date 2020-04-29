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
package com.tencent.angel.spark.ml.graph.connectedcomponent.scc

import com.tencent.angel.ml.math2.vector.{LongIntVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class SCCPSModel(colorPS: ColorPSModel, tagPS: TagPSModel) extends Serializable {
  val colorPSModel: ColorPSModel = colorPS
  val tagPSModel: TagPSModel = tagPS
  
  def numMsgs(): Long = {
    colorPSModel.numMsgs()
  }

}

class ColorPSModel(var inMsgs: PSVector,
                   var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension
  
  def initMsgs(msgs: Vector): Unit = {
    inMsgs.update(msgs)
  }
  
  def readMsgs(nodes: Array[Long]): LongLongVector = {
    inMsgs.pull(nodes).asInstanceOf[LongLongVector]
  }
  
  def readAllMsgs(): LongLongVector = {
    inMsgs.pull().asInstanceOf[LongLongVector]
  }
  
  def writeMsgs(msgs: Vector): Unit = {
    inMsgs.update(msgs)
  }
  
  def numMsgs(): Long = {
    VectorUtils.nnz(inMsgs)
  }

}

class TagPSModel(var inMsgs: PSVector,
                 var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension
  
  def initMsgs(msgs: Vector): Unit = {
    inMsgs.update(msgs)
  }
  
  def readMsgs(nodes: Array[Long]): LongIntVector = {
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]
  }
  
  def readAllMsgs(): LongIntVector = {
    inMsgs.pull().asInstanceOf[LongIntVector]
  }
  
  def writeMsgs(msgs: Vector): Unit = {
    inMsgs.update(msgs)
  }
  
  def numMsgs(): Long = {
    VectorUtils.nnz(inMsgs)
  }

}

object SCCPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
                 useBalancePartition: Boolean, balancePartitionPercent: Float): SCCPSModel = {
    val cMatrix = new MatrixContext("Colors", 2, minId, maxId)
    cMatrix.setValidIndexNum(-1)
    cMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
  
    val tMatrix = new MatrixContext("Tags", 2, minId, maxId)
    tMatrix.setValidIndexNum(-1)
    tMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, cMatrix, balancePartitionPercent)
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, tMatrix, balancePartitionPercent)
    }
    
    PSAgentContext.get().getMasterClient.createMatrix(cMatrix, 10000L)
    PSAgentContext.get().getMasterClient.createMatrix(tMatrix, 10000L)
  
    val cMatrixId = PSAgentContext.get().getMasterClient.getMatrix("Colors").getId
    val tMatrixId = PSAgentContext.get().getMasterClient.getMatrix("Tags").getId
    val cPS = new ColorPSModel(new PSVectorImpl(cMatrixId, 0, maxId, cMatrix.getRowType),
      new PSVectorImpl(cMatrixId, 1, maxId, cMatrix.getRowType))
    val tPS = new TagPSModel(new PSVectorImpl(tMatrixId, 0, maxId, tMatrix.getRowType),
      new PSVectorImpl(tMatrixId, 1, maxId, tMatrix.getRowType))
    new SCCPSModel(cPS, tPS)
  }
}
