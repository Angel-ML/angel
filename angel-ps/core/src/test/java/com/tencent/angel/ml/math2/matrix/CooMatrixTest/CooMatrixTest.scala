package com.tencent.angel.ml.math2.matrix.CooMatrixTest

import com.tencent.angel.ml.math2.MFactory
import org.scalatest.FunSuite

class CooMatrixTest extends FunSuite{

  val rowIndices = Array[Int](0, 0, 1, 2, 2, 2)
  val colIndices = Array[Int](0, 2, 2, 0, 1, 2)
  val doubleValues = Array[Double](1, 2, 3, 4, 5, 6)
  val floatValues = Array[Float](1, 2, 3, 4, 5, 6)

  test("double") {
    val mat = MFactory.cooDoubleMatrix(rowIndices, colIndices, doubleValues, Array(3, 3))
    val rowVec = mat.getRow(0)
    val colVec = mat.getCol(2)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("float") {
    val mat = MFactory.cooFloatMatrix(rowIndices, colIndices, floatValues, Array(3, 3))
    val rowVec = mat.getRow(2)
    val colVec = mat.getCol(0)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

}
