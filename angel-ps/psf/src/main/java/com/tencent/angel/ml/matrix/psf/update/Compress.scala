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


package com.tencent.angel.ml.matrix.psf.update

import scala.collection.JavaConversions._

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector.LongDoubleVector
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector._

class Compress(param: MMUpdateParam) extends MMUpdateFunc(param) {
  val defaultFilterLimit = 1e-11

  def this(matrixId: Int, rowId: Int) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double]()))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    rows.foreach {
      case r: ServerLongDoubleRow =>
        val newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[LongDoubleVector]
        r.getIter.foreach { entry =>
          if (entry.getDoubleValue < defaultFilterLimit) newSplit.set(entry.getLongKey, entry.getDoubleValue)
        }
        r.setSplit(newSplit)
      case r: ServerLongFloatRow =>
        val newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[LongDoubleVector]
        r.getIter.foreach { entry =>
          if (entry.getFloatValue < defaultFilterLimit) newSplit.set(entry.getLongKey, entry.getFloatValue)
        }
        r.setSplit(newSplit)
      case r: ServerLongLongRow =>
        val newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[LongDoubleVector]
        r.getIter.foreach { entry =>
          if (entry.getLongValue < defaultFilterLimit) newSplit.set(entry.getLongKey, entry.getLongValue)
        }
        r.setSplit(newSplit)
      case r: ServerLongIntRow =>
        val newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[LongDoubleVector]
        r.getIter.foreach { entry =>
          if (entry.getIntValue < defaultFilterLimit) newSplit.set(entry.getLongKey, entry.getIntValue)
        }
        r.setSplit(newSplit)
      case r => throw new AngelException(s"not implemented for ${r.getRowType}")
    }
  }
}