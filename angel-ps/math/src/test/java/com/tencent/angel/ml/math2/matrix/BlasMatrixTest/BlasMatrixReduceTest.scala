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


package com.tencent.angel.ml.math2.matrix.BlasMatrixTest

import java.util

import breeze.linalg.{DenseMatrix, sum}
import breeze.stats._
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix.Matrix
import org.junit.{BeforeClass, Test}

object BlasMatrixReduceTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 1000
  val dim: Int = capacity * 100
  val numRows: Int = 100
  val numCols: Int = 1000

  val intrandIndices: Array[Int] = new Array[Int](capacity)
  val longrandIndices: Array[Long] = new Array[Long](capacity)
  val intsortedIndices: Array[Int] = new Array[Int](capacity)
  val longsortedIndices: Array[Long] = new Array[Long](capacity)

  val intValues: Array[Int] = new Array[Int](capacity)
  val longValues: Array[Long] = new Array[Long](capacity)
  val floatValues: Array[Float] = new Array[Float](capacity)
  val doubleValues: Array[Double] = new Array[Double](capacity)

  val denseintValues: Array[Int] = new Array[Int](dim)
  val denselongValues: Array[Long] = new Array[Long](dim)
  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)


  val matrixlist = new util.ArrayList[Matrix]()
  var densematrix1 = MFactory.denseDoubleMatrix(numRows, numCols, densedoubleValues)
  var densematrix2 = MFactory.denseFloatMatrix(numRows, numCols, densefloatValues)
  var densematrix3 = MFactory.denseDoubleMatrix(matrixId, clock, numRows, numCols, densedoubleValues)
  var densematrix4 = MFactory.denseFloatMatrix(matrixId, clock, numRows, numCols, densefloatValues)

  var bdensematrix1 = new DenseMatrix[Double](numRows, numCols, densedoubleValues)
  var bdensematrix2 = new DenseMatrix[Float](numRows, numCols, densefloatValues)

  @BeforeClass
  def init(): Unit = {
    val rand = new util.Random()
    val set = new util.HashSet[Int]()
    var idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        intrandIndices(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        longrandIndices(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(intrandIndices, 0, intsortedIndices, 0, capacity)
    util.Arrays.sort(intsortedIndices)

    System.arraycopy(longrandIndices, 0, longsortedIndices, 0, capacity)
    util.Arrays.sort(longsortedIndices)

    doubleValues.indices.foreach { i =>
      doubleValues(i) = rand.nextDouble()
    }

    floatValues.indices.foreach { i =>
      floatValues(i) = rand.nextFloat()
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(100);
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(100)
    }

    densedoubleValues.indices.foreach { j =>
      densedoubleValues(j) = rand.nextDouble() + 0.01
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat() + 0.01f
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(100) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(100) + 1
    }

    densematrix1 = MFactory.denseDoubleMatrix(numRows, numCols, densedoubleValues)
    densematrix2 = MFactory.denseFloatMatrix(numRows, numCols, densefloatValues)
    densematrix3 = MFactory.denseDoubleMatrix(matrixId, clock, numRows, numCols, densedoubleValues)
    densematrix4 = MFactory.denseFloatMatrix(matrixId, clock, numRows, numCols, densefloatValues)

    bdensematrix1 = new DenseMatrix[Double](numRows, numCols, densedoubleValues)
    bdensematrix2 = new DenseMatrix[Float](numRows, numCols, densefloatValues)
  }

}

class BlasMatrixReduceTest {
  var densematrix1 = BlasMatrixReduceTest.densematrix1
  var densematrix2 = BlasMatrixReduceTest.densematrix2
  var densematrix3 = BlasMatrixReduceTest.densematrix3
  var densematrix4 = BlasMatrixReduceTest.densematrix4

  var bdensematrix1 = BlasMatrixReduceTest.bdensematrix1
  var bdensematrix2 = BlasMatrixReduceTest.bdensematrix2

  @Test
  def BlasDoubleMatrixTest() {
    println(s"sum: ${densematrix1.sum()}, average: ${densematrix1.average()}, std: ${densematrix1.std()}, norm: ${densematrix1.norm()}")
    println(s"sum: ${densematrix3.sum()}, average: ${densematrix3.average()}, std: ${densematrix3.std()}, norm: ${densematrix3.norm()}")
    println(s"sum: ${sum(bdensematrix1)}, average: ${mean(bdensematrix1)}, std: ${stddev(bdensematrix1)}, norm: ${}")
  }

  @Test
  def BlasFloatMatrixTest() {
    println(s"sum: ${densematrix2.sum()}, average: ${densematrix2.average()}, std: ${densematrix2.std()}, norm: ${densematrix2.norm()}")
    println(s"sum: ${densematrix4.sum()}, average: ${densematrix4.average()}, std: ${densematrix4.std()}, norm: ${densematrix4.norm()}")
    println(s"sum: ${sum(bdensematrix2)}, average: ${mean(bdensematrix2)}, std: ${stddev(bdensematrix2)}, norm: ${}")
  }

}
