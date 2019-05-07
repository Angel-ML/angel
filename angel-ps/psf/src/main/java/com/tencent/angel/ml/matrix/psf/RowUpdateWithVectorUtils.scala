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


package com.tencent.angel.ml.matrix.psf

import io.netty.buffer.ByteBuf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.RowType._
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector._

object RowUpdateWithVectorUtils {
  def update(part: RowBasedPartition,
      buf: ByteBuf,
      double2Double: (Double, Double) => Double,
      float2Float: (Float, Float) => Float,
      long2Long: (Long, Long) => Long,
      int2Int: (Int, Int) => Int): Unit = {
    val rowNum = buf.readInt
    for (_ <- 0 until rowNum) {
      val rowId = buf.readInt()
      val rowType = RowType.valueOf(buf.readInt())
      val row = part.getRow(rowId)
      row match {
        case r: ServerIntDoubleRow =>
          updateIntDoubleRow(r, rowType, buf, double2Double)
        case r: ServerIntFloatRow =>
          updateIntFloatRow(r, rowType, buf, float2Float)
        case r: ServerIntLongRow =>
          updateIntLongRow(r, rowType, buf, long2Long)
        case r: ServerIntIntRow =>
          updateIntIntRow(r, rowType, buf, int2Int)
        case r: ServerLongDoubleRow =>
          updateLongDoubleRow(r, rowType, buf, double2Double)
        case r: ServerLongFloatRow =>
          updateLongFloatRow(r, rowType, buf, float2Float)
        case r: ServerLongLongRow =>
          updateLongLongRow(r, rowType, buf, long2Long)
        case r: ServerLongIntRow =>
          updateLongIntRow(r, rowType, buf, int2Int)
      }
    }
  }

  private def updateIntDoubleRow(row: ServerIntDoubleRow,
      rowType: RowType,
      buf: ByteBuf,
      double2Double: (Double, Double) => Double) {
    row.startWrite()
    try {
      rowType match {
        case T_DOUBLE_SPARSE | T_DOUBLE_SPARSE_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = (row.getStartCol + buf.readInt()).toInt
            row.set(index, double2Double(row.get(index), buf.readDouble()))
          }
        case T_DOUBLE_DENSE | T_DOUBLE_DENSE_COMPONENT =>
          assert(buf.readInt() == row.getEndCol - row.getStartCol, "size unmatched")
          for (i <- row.getStartCol.toInt until row.getEndCol.toInt) {
            row.set(i, double2Double(row.get(i), buf.readDouble()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerIntDoubleRow")
      }
    } finally {
      row.endWrite()
    }
  }

  private def updateIntFloatRow(row: ServerIntFloatRow,
      rowType: RowType,
      buf: ByteBuf,
      floatToFloat: (Float, Float) => Float) {
    row.startWrite()
    try {
      rowType match {
        case T_FLOAT_SPARSE | T_FLOAT_SPARSE_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = (row.getStartCol + buf.readInt()).toInt
            row.set(index, floatToFloat(row.get(index), buf.readFloat()))
          }
        case T_FLOAT_DENSE | T_FLOAT_DENSE_COMPONENT =>
          assert(buf.readInt() == row.getEndCol - row.getStartCol, "size unmatched")
          for (i <- row.getStartCol.toInt until row.getEndCol.toInt) {
            row.set(i, floatToFloat(row.get(i), buf.readFloat()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerIntFloatRow")
      }
    } finally {
      row.endWrite()
    }
  }


  private def updateIntLongRow(row: ServerIntLongRow,
      rowType: RowType,
      buf: ByteBuf,
      longToLong: (Long, Long) => Long) {
    row.startWrite()
    try {
      rowType match {
        case T_LONG_SPARSE | T_LONG_SPARSE_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = (row.getStartCol + buf.readInt()).toInt
            row.set(index, longToLong(row.get(index), buf.readLong()))
          }
        case T_LONG_DENSE | T_LONG_DENSE_COMPONENT =>
          assert(buf.readInt() == row.getEndCol - row.getStartCol, "size unmatched")
          for (i <- row.getStartCol.toInt until row.getEndCol.toInt) {
            row.set(i, longToLong(row.get(i), buf.readLong()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerIntLongRow")
      }
    } finally {
      row.endWrite()
    }
  }

  private def updateIntIntRow(row: ServerIntIntRow,
      rowType: RowType,
      buf: ByteBuf,
      intToInt: (Int, Int) => Int) {
    row.startWrite()
    try {
      rowType match {
        case T_INT_SPARSE | T_INT_SPARSE_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = (row.getStartCol + buf.readInt()).toInt
            row.set(index, intToInt(row.get(index), buf.readInt()))
          }
        case T_INT_DENSE | T_INT_DENSE_COMPONENT =>
          assert(buf.readInt() == row.getEndCol - row.getStartCol, "size unmatched")
          for (i <- row.getStartCol.toInt until row.getEndCol.toInt) {
            row.set(i, intToInt(row.get(i), buf.readInt()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerIntIntRow")
      }
    } finally {
      row.endWrite()
    }
  }

  private def updateLongDoubleRow(row: ServerLongDoubleRow,
      rowType: RowType,
      buf: ByteBuf,
      double2Double: (Double, Double) => Double): Unit = {
    row.startWrite()
    try {
      rowType match {
        case T_DOUBLE_SPARSE_LONGKEY | T_DOUBLE_SPARSE_LONGKEY_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = buf.readLong() + row.getStartCol
            row.set(index, double2Double(row.get(index), buf.readDouble()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerLongDoubleRow")
      }
    } finally {
      row.endWrite()
    }
  }

  private def updateLongFloatRow(row: ServerLongFloatRow,
      rowType: RowType,
      buf: ByteBuf,
      float2Float: (Float, Float) => Float): Unit = {
    row.startWrite()
    try {
      rowType match {
        case T_FLOAT_SPARSE_LONGKEY | T_FLOAT_SPARSE_LONGKEY_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = buf.readLong() + row.getStartCol
            row.set(index, float2Float(row.get(index), buf.readFloat()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerLongFloatRow")
      }
    }
  }

  private def updateLongLongRow(row: ServerLongLongRow,
      rowType: RowType,
      buf: ByteBuf,
      long2Long: (Long, Long) => Long): Unit = {
    row.startWrite()
    try {
      rowType match {
        case T_LONG_SPARSE_LONGKEY | T_LONG_SPARSE_LONGKEY_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = buf.readLong() + row.getStartCol
            row.set(index, long2Long(row.get(index), buf.readLong()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerLongLongRow")
      }
    } finally {
      row.endWrite()
    }
  }

  private def updateLongIntRow(row: ServerLongIntRow,
      rowType: RowType,
      buf: ByteBuf,
      int2Int: (Int, Int) => Int): Unit = {
    row.startWrite()
    try {
      rowType match {
        case T_INT_SPARSE_LONGKEY | T_INT_SPARSE_LONGKEY_COMPONENT =>
          for (_ <- 0 until buf.readInt()) {
            val index = buf.readLong() + row.getStartCol
            row.set(index, int2Int(row.get(index), buf.readInt()))
          }
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operation: update $rowType to ServerLongIntRow")
      }
    } finally {
      row.endWrite();
    }
  }

}
