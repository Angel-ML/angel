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


package com.tencent.angel.spark.automl

import breeze.linalg.{DenseMatrix, DenseVector}
import com.tencent.angel.spark.automl.tuner.math.SquareDist
import org.junit.Assert._
import org.junit._

class SquareDistTest {

  @Test def testXX1D = {

    val x = DenseVector(1.0, 2.0, 3.0).toDenseMatrix.t
    val expected = DenseMatrix((0.0, 1.0, 4.0), (1.0, 0.0, 1.0), (4.0, 1.0, 0.0))
    assertEquals(expected, SquareDist(x, x))
  }

  @Test def testXX2D = {

    val x = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)).t
    val expected = DenseMatrix((0.0, 2.0, 8.0), (2.0, 0.0, 2.0), (8.0, 2.0, 0.0))
    assertEquals(expected, SquareDist(x, x))
  }

  @Test def testXY1D = {

    val x1 = DenseVector(1.0, 2.0, 3.0).toDenseMatrix.t
    val x2 = DenseVector(4.0, 5.0).toDenseMatrix.t

    val expected = DenseMatrix((9.0, 16.0), (4.0, 9.0), (1.0, 4.0))
    assertEquals(expected, SquareDist(x1, x2))
  }

  @Test def testXY2D = {

    val x1 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)).t
    val x2 = DenseMatrix((7.0, 8.0), (9.0, 10.0)).t

    val expected = DenseMatrix((61.0, 85.0), (41.0, 61.0), (25.0, 41.0))
    assertEquals(expected, SquareDist(x1, x2))
  }
}
