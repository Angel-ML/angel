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

package com.tencent.angel.graph.rank.pagerank

import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntLongVector, LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam
import com.tencent.angel.graph.psf.pagerank.{ComputeRank, GetNodes, MyIncrement, NormalizeRank}
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils

private[pagerank]
class PageRankModel(readMsgs: PSVector,
                    writeMsgs: PSVector,
                    ranks: PSVector) extends Serializable {

  val dim = readMsgs.dimension
  val matrixId = readMsgs.poolId

  def initRanks(values: Vector): Unit =
    ranks.update(values)

  def sendMsgs(msgs: Vector): Unit = {
    msgs.setRowId(writeMsgs.id)
    writeMsgs.psfUpdate(new MyIncrement(new IncrementRowsParam(writeMsgs.poolId, Array(msgs)))).get()
  }

  def readMsgs(nodes: Array[Long]): LongFloatVector =
    readMsgs.pull(nodes).asInstanceOf[LongFloatVector]

  def readAllMsgs(): LongFloatVector =
    readMsgs.pull().asInstanceOf[LongFloatVector]

  def computeRanks(initRanks: Float, resetProb: Float): Unit = {
    val func = new ComputeRank(readMsgs.poolId,
      Array(readMsgs.id, writeMsgs.id, ranks.id),
      initRanks, resetProb)

    readMsgs.psfUpdate(func).get()
  }

  def readRanks(nodes: Array[Long]): LongFloatVector =
    ranks.pull(nodes).asInstanceOf[LongFloatVector]

  def updateRanks(values: Vector): Unit =
    ranks.update(values)

  def normalizeRanks(numNodes: Long): Unit = {
    println(s"normalize ranks to the number of vertex")
    val rankSum = VectorUtils.sum(ranks)
    println(s"ranksum=$rankSum numNodes=$numNodes")
    ranks.psfUpdate(new NormalizeRank(ranks.poolId, ranks.id, rankSum, numNodes.toFloat)).get()
    println(s"after norm ranksum=${VectorUtils.sum(ranks)}")
  }

  def numMsgs(): Long =
    VectorUtils.nnz(readMsgs)

  def numNodes(): Long =
    VectorUtils.nnz(ranks)

  def getNodes(partitionIds: Array[Int], numBatch: Int): Iterator[Array[Long]] = {
    val batchSize = math.max(partitionIds.length / numBatch, 1)
    var start = 0

    new Iterator[Array[Long]] with Serializable {
      def hasNext: Boolean = start < partitionIds.length

      def next: Array[Long] = {
        val func = new GetNodes(matrixId, partitionIds.slice(start, start + batchSize))
        val nodes = ranks.psfGet(func).asInstanceOf[GetRowResult].getRow.asInstanceOf[IntLongVector]
          .getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
        start += batchSize
        nodes
      }
    }
  }
}
