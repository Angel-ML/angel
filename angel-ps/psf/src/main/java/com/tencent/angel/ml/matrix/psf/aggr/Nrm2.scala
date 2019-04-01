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

import com.tencent.angel.ml.math2.vector.{DoubleVector, FloatVector, IntVector, LongVector}
import com.tencent.angel.ml.matrix.psf.aggr.enhance.{ScalarAggrResult, ScalarPartitionAggrResult, UnaryAggrFunc}
import com.tencent.angel.ml.matrix.psf.get.base.{GetResult, PartitionGetResult}
import com.tencent.angel.ps.storage.vector.{ServerRow, ServerRowUtils};

/**
  * `Nrm2` will return 2-Norm of the `rowId` row in `matrixId` matrix.
  * Row is a Array of double, and `Nrm2` is \sqrt (\sum { row(i) * row(i) })
  */
class Nrm2(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {
  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    ServerRowUtils.getVector(row) match {
      case s: DoubleVector => Math.pow(s.norm(), 2)
      case s: FloatVector => Math.pow(s.norm(), 2)
      case s: LongVector => Math.pow(s.norm(), 2)
      case s: IntVector => Math.pow(s.norm(), 2)
    }
  }

  override protected def mergeInit: Double = 0.0

  override protected def mergeOp(a: Double, b: Double): Double = a + b

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    var aggRet = mergeInit
    val retIter = partResults.iterator()
    while (retIter.hasNext) {
      val partRet = retIter.next()
      if (null != partRet)
        aggRet = mergeOp(aggRet, partRet.asInstanceOf[ScalarPartitionAggrResult].result)
    }
    new ScalarAggrResult(Math.sqrt(aggRet))
  }
}
