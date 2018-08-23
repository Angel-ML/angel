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


package com.tencent.angel.ml.matrix.psf.update.enhance.zip2

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.psf.update.enhance.MFUpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.func.Zip2MapWithIndexFunc
import com.tencent.angel.ps.storage.vector._


/**
  * It is a Zip2MapWithIndex function which applies `Zip2MapWithIndexFunc` to `fromId1` and
  * `fromId2` row and saves the result to `toId` row.
  */
class Zip2MapWithIndex(matrixId: Int, fromId1: Int, fromId2: Int, toId: Int, func: Zip2MapWithIndexFunc)
  extends MFUpdateFunc(matrixId, Array(fromId1, fromId2, toId), func) {
  def this() = this(-1, -1, -1, -1, null)

  override def update(rows: Array[ServerRow], func: Serialize): Unit = {
    val mapper = func.asInstanceOf[Zip2MapWithIndexFunc]
    rows(2) match {
      case toRow: ServerIntDoubleRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntDoubleVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntDoubleVector]
        val to = VFactory.denseDoubleVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach { i =>
          to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)))
        }
        toRow.setSplit(to)

      case toRow: ServerIntFloatRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntFloatVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntFloatVector]
        val to = VFactory.denseFloatVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach { i =>
          to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toFloat)
        }
        toRow.setSplit(to)

      case toRow: ServerIntLongRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntLongVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntLongVector]
        val to = VFactory.denseLongVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach { i =>
          to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toLong)
        }
        toRow.setSplit(to)

      case toRow: ServerIntIntRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntIntVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntIntVector]
        val to = VFactory.denseIntVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach { i =>
          to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toInt)
        }
        toRow.setSplit(to)

      case toRow: ServerLongDoubleRow =>
        val from1 = rows(0).getSplit.asInstanceOf[LongDoubleVector]
        val from2 = rows(1).getSplit.asInstanceOf[LongDoubleVector]
        val to = VFactory.sparseLongKeyDoubleVector(from1.getDim)
        val startCol = rows(0).getStartCol
        val iter = to.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key)))
        }
        toRow.setSplit(to)

      case toRow: ServerLongFloatRow =>
        val from1 = rows(0).getSplit.asInstanceOf[LongFloatVector]
        val from2 = rows(1).getSplit.asInstanceOf[LongFloatVector]
        val to = VFactory.sparseLongKeyFloatVector(from1.getDim)
        val startCol = rows(0).getStartCol
        val iter = to.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key)).toFloat)
        }
        toRow.setSplit(to)

      case toRow: ServerLongLongRow =>
        val from1 = rows(0).getSplit.asInstanceOf[LongLongVector]
        val from2 = rows(1).getSplit.asInstanceOf[LongLongVector]
        val to = VFactory.sparseLongKeyLongVector(from1.getDim)
        val startCol = rows(0).getStartCol
        val iter = to.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key)).toLong)
        }
        toRow.setSplit(to)

      case toRow: ServerLongIntRow =>
        val from1 = rows(0).getSplit.asInstanceOf[LongIntVector]
        val from2 = rows(1).getSplit.asInstanceOf[LongIntVector]
        val to = VFactory.sparseLongKeyIntVector(from1.getDim)
        val startCol = rows(0).getStartCol
        val iter = to.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = entry.getLongKey
          entry.setValue(mapper.call(key + startCol, from1.get(key), from2.get(key)).toInt)
        }
        toRow.setSplit(to)
    }
  }
}
