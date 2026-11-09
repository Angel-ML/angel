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
package com.tencent.angel.graph.client.node2vec.params

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base.{GetParam, PartitionGetParam}
import com.tencent.angel.psagent.PSAgentContext


abstract class GetParamWithKeyIdsNumPart(matrixId: Int) extends GetParam {

  protected var keyIds: Array[Long] = _ // pls. make sure keyIds is sorted
  protected var numParts: Int = _

  //def this() = this(-1)

  def this(matrixId: Int, keyIds: Array[Long], numParts: Int) {
    this(matrixId)
    this.keyIds = keyIds
    this.numParts = numParts
  }

  override def split: util.List[PartitionGetParam] = {
    val parts = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
    val size = parts.size
    val partParams = new util.ArrayList[PartitionGetParam](size)
    var nodeIndex = 0
    import scala.collection.convert.ImplicitConversions._
    for (part <- parts) {
      val start = nodeIndex // include start
      while ( {
        nodeIndex < keyIds.length && keyIds(nodeIndex) < part.getEndCol
      }) nodeIndex += 1
      val end = nodeIndex // exclude end
      if (end - start > 0) partParams.add(getPartitionParam(part, start, end, numParts))
    }
    partParams
  }

  protected def getPartitionParam(part: PartitionKey, startIdx: Int, endIdx: Int, numParts: Int): PartitionGetParam
}
