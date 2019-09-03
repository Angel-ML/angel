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
package com.tencent.angel.ml.math2.matrix.CooMatrixTest

import com.tencent.angel.ml.math2.MFactory
import org.scalatest.FunSuite

class CooMatrixTest extends FunSuite{

  val rowIndices = Array[Int](0, 0, 1, 2, 2, 2)
  val colIndices = Array[Int](0, 2, 2, 0, 1, 2)
  val doubleValues = Array[Double](1, 2, 3, 4, 5, 6)
  val floatValues = Array[Float](1, 2, 3, 4, 5, 6)

  test("double") {
    val mat = MFactory.cooIntDoubleMatrix(rowIndices, colIndices, doubleValues, Array(3, 3))
    val rowVec = mat.getRow(0)
    val colVec = mat.getCol(2)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

  test("float") {
    val mat = MFactory.cooIntFloatMatrix(rowIndices, colIndices, floatValues, Array(3, 3))
    val rowVec = mat.getRow(2)
    val colVec = mat.getCol(0)

    println(rowVec.sum(), rowVec.dim(), rowVec.getSize)
    println(colVec.sum(), colVec.dim(), colVec.getSize)
  }

}
