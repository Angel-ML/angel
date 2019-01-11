package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.VectorUtils

import scala.reflect.ClassTag

object MathUtils {
  def rowDot[T: ClassTag](features: Matrix, weight: Matrix, bias: Vector): Matrix = {
    val tpe = implicitly[ClassTag[T]].runtimeClass

    val numRows = features.getNumRows
    val numCols = weight.getNumRows

    if (tpe == classOf[Double]) {
      val resMat = MFactory.denseDoubleMatrix(numRows, numCols)
      (0 until numCols).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
        val col = features.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
        resMat.setCol(colId, col)
      }
      resMat
    } else if (tpe == classOf[Float]) {
      val resMat = MFactory.denseFloatMatrix(numRows, numCols)
      (0 until numCols).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
        val col = features.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
        resMat.setCol(colId, col)
      }
      resMat
    } else {
      throw MLException("Data type is not support!")
    }
  }

}
