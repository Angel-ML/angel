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
package com.tencent.angel.spark.ml.graph.algo.feature.neighborVectorDistance

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetElementResult
import com.tencent.angel.graph.model.general.get.GeneralGet
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{FloatArrayElement, IElement}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class neighborVectorDistancePSModel(var nodeVectors: PSMatrix) extends Serializable {

  def initQueries(queries: Array[(Long, Array[Float])]): Unit = {
    val vectorElems = new Array[IElement](queries.length)
    val nodeIds = queries.map(_._1)

    for (i <- queries.indices) {
      vectorElems(i) = new FloatArrayElement(queries(i)._2)
    }

    nodeVectors.psfUpdate(new GeneralInit(new LongKeysUpdateParam(nodeVectors.id, nodeIds, vectorElems))).get()
  }

  def initQueries(queries: RDD[(Long, Array[Float])], batchSize: Int): Unit = {
    queries.foreachPartition { iter =>
      iter.sliding(batchSize, batchSize).foreach { pairs =>
        initQueries(pairs.toArray)
      }
    }
  }

  def readQueries(nodes: Array[Long]): Long2ObjectOpenHashMap[FloatArrayElement] = {
    nodeVectors.psfGet(
      new GeneralGet(
        new LongKeysGetParam(nodeVectors.id, nodes))).asInstanceOf[GetElementResult].getData.asInstanceOf[Long2ObjectOpenHashMap[FloatArrayElement]]
  }
}

object neighborVectorDistancePSModel {
  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float): neighborVectorDistancePSModel = {
    val nodeVectorsMatrix = ModelContextUtils.createMatrixContext(modelContext, "nodeDistance", RowType.T_ANY_LONGKEY_SPARSE, classOf[FloatArrayElement])

    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      LoadBalancePartitioner.partition(data, modelContext.getMaxNodeId, modelContext.getPartitionNum, nodeVectorsMatrix, balancePartitionPercent)
      val Parts = nodeVectorsMatrix.getParts
      nodeVectorsMatrix.setParts(Parts)
    }

    val nodeVectorsPsMatrix = PSMatrix.matrix(nodeVectorsMatrix)
    new neighborVectorDistancePSModel(nodeVectorsPsMatrix)
  }
}
