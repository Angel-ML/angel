package com.tencent.angel.ml.math2.matrix.CsrMatrixTest

import com.tencent.angel.ml.math2.MFactory
import org.scalatest.FunSuite

class CsrMatrixTest extends FunSuite{
  val rowIndices = Array[Int](0, 0, 1, 2, 2, 2)
  val colIndices = Array[Int](0, 2, 2, 0, 1, 2)
  val indptr = Array[Int](0, 2, 3, 6)
  val indices = Array[Int](0, 2, 2, 0, 1, 2)
  val doubleValues = Array[Double](1, 2, 3, 4, 5, 6)
  val floatValues = Array[Float](1, 2, 3, 4, 5, 6)

  test("double (row, col)") {
    val mat = MFactory.csrDoubleMatrix(rowIndices, colIndices, doubleValues, Array(3, 3))
    val rowVec = mat.getRow(0)
    val colVec = mat.getCol(1)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize, mat.getShape.toList.toString)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("float (row, col)") {
    val mat = MFactory.csrFloatMatrix(rowIndices, colIndices, floatValues, Array(3, 3))
    val rowVec = mat.getRow(0)
    val colVec = mat.getCol(1)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize, mat.getShape.toList.toString)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("double (indices, indptr)") {
    val mat = MFactory.csrDoubleMatrix(doubleValues, indices, indptr, Array(3, 3))
    val rowVec = mat.getRow(2)
    val colVec = mat.getCol(2)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize, mat.getShape.toList.toString)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("float (indices, indptr)") {
    val mat = MFactory.csrFloatMatrix(floatValues, indices, indptr,Array(3, 3))
    val rowVec = mat.getRow(0)
    val colVec = mat.getCol(1)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize, mat.getShape.toList.toString)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }
}
