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


package com.tencent.angel.ml.matrix.psf.update.enhance.zip3

import com.tencent.angel.common.Serialize
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, LongFloatVector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MFUpdateFunc, MFUpdateParam}
import com.tencent.angel.ps.storage.vector._


/**
  * It is a Zip3MapWithIndex function which applies `Zip3MapWithIndexFunc` to `fromId1`, `fromId2`
  * and `fromId3` row, and saves the result to `toId` row.
  */
class Zip3MapWithIndex(param: MFUpdateParam) extends MFUpdateFunc(param) {
  def this(matrixId: Int, fromId1: Int, fromId2: Int, fromId3: Int, toId: Int, func: Zip3MapWithIndexFunc) =
    this(new MFUpdateParam(matrixId, Array[Int](fromId1, fromId2, fromId3, toId), func))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], func: Serialize): Unit = {
    rows(0) match {
      case _: ServerIntDoubleRow => doUpdate(rows.map(_.asInstanceOf[ServerIntDoubleRow]), func)
      case _: ServerLongDoubleRow => doUpdate(rows.map(_.asInstanceOf[ServerLongDoubleRow]), func)
      case _: ServerIntFloatRow => doUpdate(rows.map(_.asInstanceOf[ServerIntFloatRow]), func)
      case _: ServerLongFloatRow => doUpdate(rows.map(_.asInstanceOf[ServerLongFloatRow]), func)
      case _ => throw new AngelException("not implement yet")
    }
  }

  private def doUpdate(rows: Array[ServerIntDoubleRow], func: Serialize): Unit = {
    val mapper = func.asInstanceOf[Zip3MapWithIndexFunc]
    val from1 = rows(0).getValues
    val from2 = rows(1).getValues
    val from3 = rows(2).getValues
    val to = rows(3).getValues
    val size = rows(0).size
    val startCol = rows(0).getStartCol
    for (i <- 0 until size)
      to(i) = mapper.call(i + startCol, from1(i), from2(i), from3(i))
  }

  //todo: to be improved
  private def doUpdate(rows: Array[ServerLongDoubleRow], func: Serialize): Unit = {
    val mapper = func.asInstanceOf[Zip3MapWithIndexFunc]
    val from1 = rows(0).getSplit.asInstanceOf[LongDoubleVector]
    val from2 = rows(1).getSplit.asInstanceOf[LongDoubleVector]
    val from3 = rows(2).getSplit.asInstanceOf[LongDoubleVector]
    val to = from1.add(from2).add(from3).asInstanceOf[LongDoubleVector]
    val startCol = rows(0).getStartCol
    val iter = to.getStorage.entryIterator
    while (iter.hasNext) {
      val entry = iter.next
      val key = entry.getLongKey
      entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key), from3.get(key)))
    }
    rows(3).setSplit(to)
  }

  private def doUpdate(rows: Array[ServerIntFloatRow], func: Serialize): Unit = {
    val mapper = func.asInstanceOf[Zip3MapWithIndexFunc]
    val from1 = rows(0).getValues
    val from2 = rows(1).getValues
    val from3 = rows(2).getValues
    val to = rows(3).getValues
    val startCol = rows(0).getStartCol
    val size = rows(0).size
    for (i <- 0 until size)
      to(i) = mapper.call(i + startCol, from1(i), from2(i), from3(i)).toFloat
  }

  //todo: to be improved
  private def doUpdate(rows: Array[ServerLongFloatRow], func: Serialize): Unit = {
    val mapper = func.asInstanceOf[Zip3MapWithIndexFunc]
    val from1 = rows(0).getSplit.asInstanceOf[LongFloatVector]
    val from2 = rows(1).getSplit.asInstanceOf[LongFloatVector]
    val from3 = rows(2).getSplit.asInstanceOf[LongFloatVector]
    val to = from1.add(from2).add(from3).asInstanceOf[LongFloatVector]
    val startCol = rows(0).getStartCol
    val iter = to.getStorage.entryIterator
    while (iter.hasNext) {
      val entry = iter.next
      val key = entry.getLongKey
      entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key), from3.get(key)).toFloat)
    }
    rows(3).setSplit(to)
  }
}
