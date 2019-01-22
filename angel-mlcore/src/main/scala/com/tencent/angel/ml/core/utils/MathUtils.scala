package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.core.network.variable.{BlasMatVariable, MatVariable, Variable}
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.VectorUtils

object MathUtils {
  def rowDot(features: Matrix, weight: MatVariable): Matrix = {
    val numRows = features.getNumRows
    val numCols = weight.getNumRows

    weight match {
      case _: BlasMatVariable => // the shape of weight matrix is (inputDim, outputDim)
        Ufuncs.dot(features, false, weight, true)
      case mat: Variable if mat.rowType.isDouble => // the shape of weight matrix is (outputDim, inputDim)
        val resMat = MFactory.denseDoubleMatrix(numRows, numCols)
        (0 until numCols).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
          val col = features.dot(weight.getRow(colId))
          resMat.setCol(colId, col)
        }
        resMat
      case mat: Variable if mat.rowType.isFloat =>
        val resMat = MFactory.denseFloatMatrix(numRows, numCols)
        (0 until numCols).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
          val col = features.dot(weight.getRow(colId))
          resMat.setCol(colId, col)
        }
        resMat
      case _ => throw MLException("Data type is not support!")
    }
  }

  def wrapVector2Matrix(nec: Vector): Matrix = {
    null
  }
}
