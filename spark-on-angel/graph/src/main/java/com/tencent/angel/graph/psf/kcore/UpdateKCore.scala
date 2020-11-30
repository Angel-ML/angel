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
package com.tencent.angel.graph.psf.kcore

import com.tencent.angel.ml.math2.vector.{IntIntVector, LongIntVector, Vector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongIntRow, ServerRow, ServerRowUtils}

class UpdateKCore(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int]) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](0)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongIntRow])
    val outMsgs = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongIntRow])
    val cores = ServerRowUtils.getVector(rows(2).asInstanceOf[ServerLongIntRow])

    (outMsgs, cores) match {
      case (o: IntIntVector, c: IntIntVector) =>
        computeCores(o, c)
      case (o: LongIntVector, c: LongIntVector) =>
        computeCores(o, c)
    }

    switchWriteAndRead(inMsgs, outMsgs)
  }

  def computeCores(writeMsgs: IntIntVector, cores: IntIntVector): Unit = {
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getIntKey
      val msg = entry.getIntValue
      cores.set(key, msg)
    }
  }

  def computeCores(writeMsgs: LongIntVector, cores: LongIntVector): Unit = {
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getLongKey
      val msg = entry.getIntValue
      cores.set(key, msg)
    }
  }

  def switchWriteAndRead(inMsgs: Vector, outMsgs: Vector): Unit = {
    val storage = inMsgs.getStorage
    storage.clear()
    inMsgs.setStorage(outMsgs.getStorage)
    outMsgs.setStorage(storage)
  }

}
