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
package com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex

import com.tencent.angel.graph.client.getNodeAttrs.{GetNodeAttrs, GetNodeAttrsParam, GetNodeAttrsResult}
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.model.attr.psf.init.InitNodeAttrs
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{FloatArrayElement, IElement}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD


class MultiAttrHIndexPSModel(val batchSize: Int, val attributes: PSMatrix,
                             modelContext: ModelContext) extends Serializable {

  def initAttributes(attrs: RDD[(Long, Array[Float])]): Long = {
    attrs.mapPartitions { iter => {
      iter.sliding(batchSize, batchSize).map(pairs => initAttrs(pairs)) }
    }.reduce(_ + _)
  }

  def initAttrs(pairs: Seq[(Long, Array[Float])]): Long = {
    val nodeIds = new Array[Long](pairs.size)
    val attrs = new Array[IElement](pairs.size)
    pairs.zipWithIndex.foreach( e => {
      nodeIds(e._2) = e._1._1
      attrs(e._2) = new FloatArrayElement(e._1._2).asInstanceOf[IElement]
    })

    val func = new InitNodeAttrs(new LongKeysUpdateParam(attributes.id, nodeIds, attrs))
    attributes.asyncPsfUpdate(func).get()
    println(s"pairs length:${pairs.length}")
    pairs.length
  }

  def readAttributes(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Float]] = {
    attributes.psfGet(new GetNodeAttrs(new GetNodeAttrsParam(attributes.id, nodeIds, -1)))
      .asInstanceOf[GetNodeAttrsResult].getNodeIdToAttrs
  }

  def checkpoint(iter: Int = 0): Unit = {
    attributes.checkpoint(iter)
  }

}

object MultiAttrHIndexPSModel {


  def fromMinMax(batchSize: Int, modelContext: ModelContext, data: RDD[Long],
                 useBalancePartition: Boolean): MultiAttrHIndexPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(
      modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[FloatArrayElement])

    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition && data != null)
      LoadBalancePartitioner.partition(data, modelContext.getPartitionNum, matrix)

    val psMatrix = PSMatrix.matrix(matrix)
    new MultiAttrHIndexPSModel(batchSize, psMatrix, modelContext)
  }
}
