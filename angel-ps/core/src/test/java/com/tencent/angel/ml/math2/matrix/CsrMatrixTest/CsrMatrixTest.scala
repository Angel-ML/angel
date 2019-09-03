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
package com.tencent.angel.ml.math2.matrix.CsrMatrixTest

import com.tencent.angel.ml.math2.MFactory
import org.scalatest.FunSuite

class CsrMatrixTest extends FunSuite{
  val rowIndices = Array[Int](1, 1, 1, 2)
  val colIndices = Array[Int](2, 0, 1, 2)
  val indptr = Array[Int](0, 0, 3, 4)
  val indices = Array[Int](2, 0, 1, 2)
  val doubleValues = Array[Double](3, 4, 5, 6)
  val floatValues = Array[Float](3, 4, 5, 6)

  test("double (row, col)") {
    val mat = MFactory.csrDoubleMatrix(rowIndices, colIndices, doubleValues, Array(3, 3))
    val rowVec = mat.getRow(2)
    val colVec = mat.getCol(2)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize, mat.getShape.toList.toString)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("float (row, col)") {
    val mat = MFactory.csrFloatMatrix(rowIndices, colIndices, floatValues, Array(3, 3))
    val rowVec = mat.getRow(2)
    val colVec = mat.getCol(2)

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
