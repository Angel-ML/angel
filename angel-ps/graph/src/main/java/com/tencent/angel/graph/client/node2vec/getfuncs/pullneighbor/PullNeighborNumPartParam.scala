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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.graph.client.node2vec.params.{GetParamWithKeyIdsNumPart, PartitionGetParamWithIdsNumParts}
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam

class PullNeighborNumPartParam(matrixId: Int, nodeIds: Array[Long], numParts: Int) extends
  GetParamWithKeyIdsNumPart(matrixId, nodeIds, numParts) {

  //def this(matrixId:Int, nodeIds:Array[Long], numParts:Int) = this(matrixId, nodeIds, numParts)

  util.Arrays.sort(keyIds)

  override protected def getPartitionParam(part: PartitionKey, startIdx: Int, endIdx: Int, numParts: Int): PartitionGetParam = { // int matrixId, PartitionKey partKey, long[] keyIds, int startIdx, int endIdx
    new PartitionGetParamWithIdsNumParts(matrixId, part, keyIds, numParts, startIdx, endIdx)
  }
}