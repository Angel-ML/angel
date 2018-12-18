package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.VectorUtils

import scala.reflect.ClassTag

object MathUtils {
  def rowDot[T: ClassTag](weight: Matrix, bias: Vector)(implicit graph: Graph): Matrix = {
    val tpe = implicitly[ClassTag[T]].runtimeClass

    val outputDim = weight.getNumRows
    val denseMat = if (tpe == classOf[Double]) {
      MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
    } else if (tpe == classOf[Float]) {
      MFactory.denseFloatMatrix(graph.placeHolder.getBatchSize, outputDim)
    } else {
      throw MLException("Data type is not support!")
    }

    (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
      val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
      if (tpe == classOf[Double]) {
        denseMat.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
      } else if (tpe == classOf[Float]) {
        denseMat.asInstanceOf[BlasFloatMatrix].setCol(colId, col)
      }
    }

    denseMat
  }

}
