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
package com.tencent.angel.graph.community.copra

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetElementResult
import com.tencent.angel.graph.model.general.get.GeneralGet
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.psf.triangle.NeighborsFloatAttrsElement
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class COPRAPSModel(var comInMsgs: PSMatrix,
                   var comOutMsgs: PSMatrix) extends Serializable {

  val dim = comInMsgs.columns

  def initComInMsgs(msgs: Long2ObjectOpenHashMap[NeighborsFloatAttrsElement]): Unit = {
    val nodeIds = new Array[Long](msgs.size())
    val neighborElems = new Array[IElement](msgs.size())

    val iter = msgs.long2ObjectEntrySet().fastIterator()
    var index = 0
    while (iter.hasNext) {
      val i = iter.next()
      nodeIds(index) = i.getLongKey
      neighborElems(index) = i.getValue
      index += 1
    }

    comInMsgs.psfUpdate(new GeneralInit(new LongKeysUpdateParam(comInMsgs.id, nodeIds, neighborElems))).get()
  }


  def writeComOutMsgs(msgs: Long2ObjectOpenHashMap[NeighborsFloatAttrsElement]): Unit = {
    val nodeIds = new Array[Long](msgs.size())
    val neighborElems = new Array[IElement](msgs.size())

    val iter = msgs.long2ObjectEntrySet().fastIterator()
    var index = 0
    while (iter.hasNext) {
      val i = iter.next()
      nodeIds(index) = i.getLongKey
      neighborElems(index) = i.getValue
      index += 1
    }

    comOutMsgs.psfUpdate(new GeneralInit(new LongKeysUpdateParam(comOutMsgs.id, nodeIds, neighborElems)))
  }

  def readComInMsgs(nodes: Array[Long]): Long2ObjectOpenHashMap[NeighborsFloatAttrsElement] = {
    comInMsgs.psfGet(
      new GeneralGet(
        new LongKeysGetParam(comInMsgs.id, nodes))).asInstanceOf[GetElementResult].getData.asInstanceOf[Long2ObjectOpenHashMap[NeighborsFloatAttrsElement]]
  }


  def resetMsgs(): Unit = {
    val temp = comInMsgs
    comInMsgs = comOutMsgs
    comOutMsgs = temp
    comOutMsgs.reset()
  }
}

object COPRAPSModel {

  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float): COPRAPSModel = {
    val comInMatrix = ModelContextUtils.createMatrixContext(modelContext, "comInMC", RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsFloatAttrsElement])
    val comOutMatrix = ModelContextUtils.createMatrixContext(modelContext, "comOutMC", RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsFloatAttrsElement])


    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      LoadBalancePartitioner.partition(data, modelContext.getMaxNodeId, modelContext.getPartitionNum, comInMatrix, balancePartitionPercent)
      val Parts = comInMatrix.getParts
      comOutMatrix.setParts(Parts)
    }

    val comInPsMatrix = PSMatrix.matrix(comInMatrix)
    val comOutPsMatrix = PSMatrix.matrix(comOutMatrix)

    new COPRAPSModel(comInPsMatrix, comOutPsMatrix)

  }

}
