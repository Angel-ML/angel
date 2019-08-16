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
package com.tencent.angel.spark.ml.psf.pagerank

import com.tencent.angel.ml.math2.vector.{FloatVector, IntFloatVector, LongFloatVector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class ComputeRank(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int], initRank: Float, resetProb: Float, resizeFlag:Float) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](initRank, resetProb, resizeFlag)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val outMsgs = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongFloatRow])
    val ranks = ServerRowUtils.getVector(rows(2).asInstanceOf[ServerLongFloatRow])
    rows(2).startWrite()
    (outMsgs, ranks) match {
      case (o: IntFloatVector, r: IntFloatVector) =>
        computeRanks(o, r, scalars(0).toFloat, scalars(1).toFloat, scalars(2).toFloat)
      case (o: LongFloatVector, r: LongFloatVector) =>
        computeRanks(o, r, scalars(0).toFloat, scalars(1).toFloat, scalars(2).toFloat)
    }
    switchWriteAndRead(inMsgs, outMsgs)
    rows(2).endWrite()
  }

  def computeRanks(writeMsgs: IntFloatVector, ranks: IntFloatVector,
                   defaultValue: Float, resetProb: Float, resizeFlag:Float): Unit = {
    if (resizeFlag > 0 && ranks.size() == 0)
      ranks.setStorage(writeMsgs.getStorage.emptySparse(writeMsgs.size()))
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getIntKey
      val msg = entry.getFloatValue
      var old = ranks.get(key)
      if (old == 0.0) old = defaultValue
      ranks.set(key, old + msg * (1 - resetProb))
    }
  }

  def computeRanks(writeMsgs: LongFloatVector, ranks: LongFloatVector,
                   defaultValue: Float, resetProb: Float, resizeFlag:Float): Unit = {
    if (resizeFlag > 0 && ranks.size() == 0)
      ranks.setStorage(writeMsgs.getStorage.emptySparse(writeMsgs.size().toInt))
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getLongKey
      val msg = entry.getFloatValue
      var old = ranks.get(key)
      if (old == 0.0) old = defaultValue
      ranks.set(key, old + msg * (1 - resetProb))
    }
  }

  def switchWriteAndRead(inMsgs: FloatVector, outMsgs: FloatVector): Unit = {
    val tmp = inMsgs.getStorage
    inMsgs.setStorage(outMsgs.getStorage)
    outMsgs.setStorage(tmp)
    tmp.clear()
  }

}
