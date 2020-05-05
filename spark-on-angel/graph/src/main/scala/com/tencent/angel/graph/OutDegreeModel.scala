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
package com.tencent.angel.graph

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.data.VertexId
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.psf.clusterrank._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * An out-degree table
  * @param param
  */
class OutDegreeModel(val param: Param) extends Serializable {
  var psMatrix: PSMatrix = _


  def init(data: RDD[(VertexId, Int)]): Unit = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName("clusterrank.out-degree")
    mc.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    mc.setRowNum(1)
    mc.setColNum(param.maxIndex)
    mc.setMaxColNumInBlock(param.maxIndex / param.psPartNum)

    psMatrix = PSMatrix.matrix(mc)

    data.foreachPartition { iter =>
      BatchIter(iter, param.batchSize).foreach { pairs =>
        val nodeIdToOutDegree = new Long2IntOpenHashMap(pairs.length)
        pairs.foreach { case (nodeId, degree) =>
            nodeIdToOutDegree.put(nodeId, degree)
        }
        val psfunc = new InitOutDegreeFunc(new InitOutDegreeParam(psMatrix.id, nodeIdToOutDegree))
        psMatrix.asyncPsfUpdate(psfunc).get()

        nodeIdToOutDegree.clear()
        println(s"push out-degree of ${pairs.length} nodes")
      }
    }
  }

  def getOutDegrees(nodeIds: Array[VertexId]): Long2IntOpenHashMap = {
    psMatrix.psfGet(new GetOutDegreeFunc(new GetOutDegreeParam(psMatrix.id, nodeIds)))
      .asInstanceOf[GetOutDegreeResult].getNodeIdToOutDegree
  }

}

object OutDegreeModel {
  def create(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int): OutDegreeModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum)
    new OutDegreeModel(param)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

}
