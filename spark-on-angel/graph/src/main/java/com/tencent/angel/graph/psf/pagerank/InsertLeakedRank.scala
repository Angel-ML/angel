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
package com.tencent.angel.graph.psf.pagerank

import com.tencent.angel.ml.math2.vector.{IntFloatVector, LongFloatVector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class InsertLeakedRank (param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, incVal: Double) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](incVal)))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val ranks = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val leakedRank = scalars(0).toFloat
    ranks match {
      case r: IntFloatVector =>
        val it = r.getStorage.entryIterator()
        while (it.hasNext) {
          val entry = it.next()
          r.set(entry.getIntKey, entry.getFloatValue + leakedRank)
        }

      case r: LongFloatVector =>
        val it = r.getStorage.entryIterator()
        while (it.hasNext) {
          val entry = it.next()
          r.set(entry.getLongKey, entry.getFloatValue + leakedRank)
        }
    }
  }

}
