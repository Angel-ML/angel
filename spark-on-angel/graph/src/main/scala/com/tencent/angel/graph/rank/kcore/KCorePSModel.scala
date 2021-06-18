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

package com.tencent.angel.graph.rank.kcore

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.psf.kcore.readTag.{ReadTag, ReadTagResult}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

/**
  * KCorePSModel implementation
  *
  * @param inMsgs  use to store the last updated node
  * @param outMsgs use to store the updated node
  */
private[kcore]
class KCorePSModel(var inMsgs: PSVector,
                   var outMsgs: PSVector,
                   var cores: PSVector,
                   val staticCores: PSVector,
                   psMatrix: PSMatrix) extends Serializable {
  
  val dim: Long = cores.dimension
  val matrixId: Int = cores.poolId
  
  def initStaticCores(values: Vector): Unit = {
    assert(staticCores != null, s"no static cores provided")
    staticCores.update(values)
  }
  
  
  def readStaticCores(nodes: Array[Long]): LongIntVector = {
    assert(staticCores != null, s"no static cores provided")
    staticCores.pull(nodes).asInstanceOf[LongIntVector]
  }
  
  
  def initCores(values: Vector): Unit =
    cores.update(values)
  
  def readCores(nodes: Array[Long]): LongIntVector =
    cores.pull(nodes).asInstanceOf[LongIntVector]
  
  def readTag(nodes: Array[Long]): Array[Long] = {
    inMsgs.psfGet(new ReadTag(matrixId, nodes, inMsgs.id)).asInstanceOf[ReadTagResult].getNodes
  }
  
  def readMsgs(nodes: Array[Long]): LongIntVector =
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]
  
  def readAllMsgs(): LongIntVector =
    inMsgs.pull().asInstanceOf[LongIntVector]
  
  def writeInMsgs(values: Vector): Unit = {
    inMsgs.update(values)
  }
  
  def writeMsgs(values: Vector): Unit =
    outMsgs.update(values)
  
  def updateCores(values: Vector): Unit =
    cores.update(values)
  
  def asyncUpdateCores(values: Vector): Unit =
    cores.asyncUpdate(values)
  
  def numCores(): Long = VectorUtils.nnz(cores)
  
  def numKeys2calc(): Long = VectorUtils.nnz(inMsgs)
  
  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)
  
  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }
  
  def resetCores(): Unit = {
    cores.reset
  }
  
  def checkpoint(n: Int): Unit = psMatrix.checkpoint(n)
}

object KCorePSModel {
  def apply(modelContext: ModelContext, edges: RDD[(Long, Long)], mode: String = "full", useBalancePartition: Boolean = false,
            balancePartitionPercent: Float = 0.7f): KCorePSModel = {
    val matrix =
      if (mode.toLowerCase == "sparse" || mode.toLowerCase == "mid") {
        ModelContextUtils.createMatrixContext(modelContext, RowType.T_INT_SPARSE_LONGKEY, 4)
      }
      else {
        ModelContextUtils.createMatrixContext(modelContext, RowType.T_INT_SPARSE_LONGKEY, 3)
      }
    
    
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.flatMap(e => Iterator(e._1, e._2))
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)
    }
    
    val psMatrix = PSMatrix.matrix(matrix)
    if (mode.toLowerCase() == "mid" || mode.toLowerCase() == "sparse") {
      new KCorePSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 1, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 2, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 3, modelContext.getMaxNodeId, matrix.getRowType), psMatrix)
    }
    else {
      new KCorePSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 1, modelContext.getMaxNodeId, matrix.getRowType),
        new PSVectorImpl(psMatrix.id, 2, modelContext.getMaxNodeId, matrix.getRowType), null, psMatrix)
    }
    
  }
  
}