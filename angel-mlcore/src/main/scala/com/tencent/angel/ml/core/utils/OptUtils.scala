package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector._

object OptUtils {

  def getRowsAsMatrix(storage: Matrix, from: Int, to: Int): Matrix = {
    storage.getRow(0).getType match {
      case RowType.T_DOUBLE_DENSE =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntDoubleVector] }
        MFactory.rbIntDoubleMatrix(rows)
      case RowType.T_DOUBLE_SPARSE =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntDoubleVector] }
        MFactory.rbIntDoubleMatrix(rows)
      case RowType.T_DOUBLE_SPARSE_LONGKEY =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[LongDoubleVector] }
        MFactory.rbLongDoubleMatrix(rows)
      case RowType.T_FLOAT_DENSE =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntFloatVector] }
        MFactory.rbIntFloatMatrix(rows)
      case RowType.T_FLOAT_SPARSE =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntFloatVector] }
        MFactory.rbIntFloatMatrix(rows)
      case RowType.T_FLOAT_SPARSE_LONGKEY =>
        val rows = (from until to).toArray.map { rId => storage.getRow(rId).asInstanceOf[LongFloatVector] }
        MFactory.rbLongFloatMatrix(rows)
      case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
    }
  }

  def getRowAsMatrix(storage: Matrix, rowId: Int, numRows: Int, numCol:Int): Matrix = {
    storage.getRow(rowId).getStorage match {
      case s: IntDoubleDenseVectorStorage =>
        MFactory.denseDoubleMatrix(numRows, numCol, s.getValues)
      case s: IntFloatDenseVectorStorage =>
        MFactory.denseFloatMatrix(numRows, numCol, s.getValues)
      case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
    }
  }

  def wrapVector2Matrix(vec: Vector): Matrix = {
    vec match {
      case v: IntDoubleVector =>
        MFactory.rbIntDoubleMatrix(Array[IntDoubleVector](v))
      case v: IntFloatVector =>
        MFactory.rbIntFloatMatrix(Array[IntFloatVector](v))
      case _ => throw MLException("Vector type is not support!")
    }
  }
}
