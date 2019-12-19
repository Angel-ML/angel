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

;

import scala.collection.JavaConversions._
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage, IntIntDenseVectorStorage, IntLongDenseVectorStorage}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.{BinaryAggrFunc, ScalarAggrResult, ScalarPartitionAggrResult}
import com.tencent.angel.ml.matrix.psf.get.base.{GetResult, PartitionGetResult}
import com.tencent.angel.ps.storage.vector._

/**
  * `Equal` judges if two rows is equal.
  */
class Equal(matrixId: Int, rowId1: Int, rowId2: Int) extends BinaryAggrFunc(matrixId: Int, rowId1: Int, rowId2: Int) {
  val equal = 1.0
  val notEqual = 0.0
  val equalLimit = 1e-10

  def this() = this(-1, -1, -1)

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    var sum = 1.0
    for (partResult <- partResults) {
      if (partResult != null) {
        sum = math.min(sum, partResult.asInstanceOf[ScalarPartitionAggrResult].result)
      }
    }
    new ScalarAggrResult(sum)
  }

  override protected def processRows(row1: ServerRow, row2: ServerRow): Double = {
    val eq = if (row1.isDense && row2.isDense) {
      row1 match {
        case r1: ServerIntDoubleRow => ServerRowUtils.getVector(r1).getStorage.asInstanceOf[IntDoubleDenseVectorStorage].getValues.zip(ServerRowUtils.getVector(row2).getStorage.asInstanceOf[IntDoubleDenseVectorStorage].getValues)
          .forall { case (a, b) => math.abs(a - b) < equalLimit }
        case r1: ServerIntFloatRow => ServerRowUtils.getVector(r1).getStorage.asInstanceOf[IntFloatDenseVectorStorage].getValues.zip(ServerRowUtils.getVector(row2).getStorage.asInstanceOf[IntFloatDenseVectorStorage].getValues)
          .forall { case (a, b) => math.abs(a - b) < equalLimit }
        case r1: ServerIntLongRow => ServerRowUtils.getVector(r1).getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues.zip(ServerRowUtils.getVector(row2).getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues)
          .forall { case (a, b) => math.abs(a - b) < equalLimit }
        case r1: ServerIntIntRow => ServerRowUtils.getVector(r1).getStorage.asInstanceOf[IntIntDenseVectorStorage].getValues.zip(ServerRowUtils.getVector(row2).getStorage.asInstanceOf[IntIntDenseVectorStorage].getValues)
          .forall { case (a, b) => math.abs(a - b) < equalLimit }
        case _ => throw new AngelException("should not come here!")
      }
    } else {
      ServerRowUtils.getVector(row1).sub(ServerRowUtils.getVector(row2)).ifilter(equalLimit).sum() == 0
    }
    if (eq) equal else notEqual
  }
}
