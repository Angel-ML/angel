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


package com.tencent.angel.ml.matrix.psf.aggr

import scala.collection.JavaConversions._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.{BinaryAggrFunc, ScalarAggrResult, ScalarPartitionAggrResult}
import com.tencent.angel.ml.matrix.psf.get.base.{GetResult, PartitionGetResult}
import com.tencent.angel.ps.storage.vector.{ServerRow, ServerRowUtils}

/**
  * `Dot` will return dot product result of `rowId1` and `rowId2`.
  * That is math.dot(matrix[rowId1], matrix[rowId2]).
  */
class Dot(matrixId: Int, rowId1: Int, rowId2: Int) extends BinaryAggrFunc(matrixId: Int, rowId1: Int, rowId2: Int) {
  def this() = this(-1, -1, -1)

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    var sum = 0.0
    for (partResult <- partResults) {
      if (partResult != null) {
        sum += partResult.asInstanceOf[ScalarPartitionAggrResult].result
      }
    }
    new ScalarAggrResult(sum)
  }

  override protected def processRows(row1: ServerRow, row2: ServerRow): Double = ServerRowUtils.getVector(row1).dot(ServerRowUtils.getVector(row2))

}
