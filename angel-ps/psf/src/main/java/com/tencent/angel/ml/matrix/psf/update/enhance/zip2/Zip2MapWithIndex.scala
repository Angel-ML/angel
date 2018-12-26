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
import com.tencent.angel.ml.math2.storage._
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
    val newSplit = rows(2) match {
      case toRow: ServerIntDoubleRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntDoubleVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntDoubleVector]
        val to = VFactory.denseDoubleVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach(i => to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i))))
        to

      case toRow: ServerIntFloatRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntFloatVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntFloatVector]
        val to = VFactory.denseFloatVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach { i =>
          to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toFloat)
        }
        to

      case toRow: ServerIntLongRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntLongVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntLongVector]
        val to = VFactory.denseLongVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach(i => to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toLong))
        to

      case toRow: ServerIntIntRow =>
        val from1 = rows(0).getSplit.asInstanceOf[IntIntVector]
        val from2 = rows(1).getSplit.asInstanceOf[IntIntVector]
        val to = VFactory.denseIntVector(from1.getDim)
        val indices = if (rows(0).isDense || rows(1).isDense) (0 until toRow.size()).toArray else
          (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
        indices.foreach(i => to.set(i, mapper.call(i + toRow.getStartCol, from1.get(i), from2.get(i)).toInt))
        to

      case toRow: ServerLongDoubleRow =>
        rows(0).getSplit.getStorage match {
          case _: IntDoubleDenseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntDoubleVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntDoubleVector]
            val to = VFactory.sparseDoubleVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val endCol = rows(0).getEndCol
            val size = (endCol - startCol).toInt
            val indices = (0 until size)
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i))))
            to

          case _: IntDoubleSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntDoubleVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntDoubleVector]
            val to = VFactory.sparseDoubleVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i))))
            to
          case _: LongDoubleSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[LongDoubleVector]
            val from2 = rows(1).getSplit.asInstanceOf[LongDoubleVector]
            val to = VFactory.sparseLongKeyDoubleVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i))))
            to
        }

      case toRow: ServerLongFloatRow =>
        rows(0).getSplit.getStorage match {
          case _: IntFloatDenseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntFloatVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntFloatVector]
            val to = VFactory.sparseFloatVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val endCol = rows(0).getEndCol
            val size = (endCol - startCol).toInt
            val indices = (0 until size)
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toFloat))
            to

          case _: IntFloatSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntFloatVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntFloatVector]
            val to = VFactory.sparseFloatVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toFloat))
            to

          case _: LongFloatSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[LongFloatVector]
            val from2 = rows(1).getSplit.asInstanceOf[LongFloatVector]
            val to = VFactory.sparseLongKeyFloatVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toFloat))
            to
        }

      case toRow: ServerLongLongRow =>
        rows(0).getSplit.getStorage match {
          case _: IntLongDenseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntLongVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntLongVector]
            val to = VFactory.sparseLongVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val endCol = rows(0).getEndCol
            val size = (endCol - startCol).toInt
            val indices = (0 until size)
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toLong))
            to

          case _: IntLongSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntLongVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntLongVector]
            val to = VFactory.sparseLongVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toLong))
            to

          case _: LongLongSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[LongLongVector]
            val from2 = rows(1).getSplit.asInstanceOf[LongLongVector]
            val to = VFactory.sparseLongKeyLongVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toLong))
            to
        }

      case toRow: ServerLongIntRow =>
        rows(0).getSplit.getStorage match {
          case _: IntIntDenseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntIntVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntIntVector]
            val to = VFactory.sparseIntVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val endCol = rows(0).getEndCol
            val size = (endCol - startCol).toInt
            val indices = (0 until size)
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toInt))
            to

          case _: IntIntSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[IntIntVector]
            val from2 = rows(1).getSplit.asInstanceOf[IntIntVector]
            val to = VFactory.sparseIntVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toInt))
            to

          case _: LongIntSparseVectorStorage =>
            val from1 = rows(0).getSplit.asInstanceOf[LongIntVector]
            val from2 = rows(1).getSplit.asInstanceOf[LongIntVector]
            val to = VFactory.sparseLongKeyIntVector(from1.getDim)
            val startCol = rows(0).getStartCol
            val indices = (from1.getStorage.getIndices ++ from2.getStorage.getIndices).distinct
            indices.foreach(i => to.set(i, mapper.call(i + startCol, from1.get(i), from2.get(i)).toInt))
            to
        }
    }
    try {
      rows(2).startWrite()
      rows(2).setSplit(newSplit)
    } finally {
      rows(2).endWrite()
    }
  }
}