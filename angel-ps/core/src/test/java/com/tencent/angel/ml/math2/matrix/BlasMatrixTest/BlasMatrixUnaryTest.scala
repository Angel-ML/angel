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

import breeze.linalg.{*, DenseMatrix, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import org.junit.{BeforeClass, Test}

object BlasMatrixUnaryTest {
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

    bdensematrix1 = new DenseMatrix[Double](numRows, numCols, densedoubleValues)
    bdensematrix2 = new DenseMatrix[Float](numRows, numCols, densefloatValues)
  }
}

class BlasMatrixUnaryTest {
  var densematrix1 = BlasMatrixUnaryTest.densematrix1
  var densematrix2 = BlasMatrixUnaryTest.densematrix2

  var bdensematrix1 = BlasMatrixUnaryTest.bdensematrix1
  var bdensematrix2 = BlasMatrixUnaryTest.bdensematrix2

  val numRows: Int = BlasMatrixUnaryTest.numRows
  val numCols: Int = BlasMatrixUnaryTest.numCols

  val densefloatValues: Array[Float] = BlasMatrixUnaryTest.densefloatValues
  val densedoubleValues: Array[Double] = BlasMatrixUnaryTest.densedoubleValues

  @Test
  def saddTest() {
    assert(Ufuncs.sadd(densematrix1, 2.0).sum() == sum(bdensematrix1 + 2.0))
    //    assert(abs(Ufuncs.sadd(densematrix2,2.0).sum() - sum(bdensematrix2 + 2.0f))< 1.0)
  }

  @Test
  def ssubTest() {
    assert(Ufuncs.ssub(densematrix1, 2.0).sum() == sum(bdensematrix1 - 2.0))
//    assert(abs(Ufuncs.ssub(densematrix2, 2.0).sum() - sum(bdensematrix2 - 2.0f)) < 1.0)
  }

  @Test
  def smulTest() {
    assert(Ufuncs.smul(densematrix1, 2.0).sum() == sum(bdensematrix1 * 2.0))
//    assert(abs(Ufuncs.smul(densematrix2, 2.0).sum() - sum(bdensematrix2 * 2.0f)) < 1.0)

  }

  @Test
  def sdivTest() {
    assert(Ufuncs.sdiv(densematrix1, 2.0).sum() == sum(bdensematrix1 / 2.0))
    assert(abs(Ufuncs.sdiv(densematrix2, 2.0).sum() - sum(bdensematrix2 / 2.0f)) < 1.0)

  }

  @Test
  def expTest() {
    assert(Ufuncs.exp(densematrix1).sum() == sum(exp(bdensematrix1)))
    //    assert(abs(Ufuncs.exp(densematrix2).sum() - sum(exp(bdensematrix2)))< 1.0)
  }

  @Test
  def logTest() {
    assert(Ufuncs.log(densematrix1).sum() == sum(log(bdensematrix1)))
    //assert(abs(Ufuncs.log(densematrix2).sum() - sum(log(bdensematrix2))) < 1.0)
  }

  @Test
  def log1pTest() {
    assert(Ufuncs.log1p(densematrix1).sum() == sum(log1p(bdensematrix1)))
    //assert(abs(Ufuncs.log1p(densematrix2).sum() - sum(log1p(bdensematrix2))) < 1.0)
  }

  @Test
  def powTest() {
    assert(Ufuncs.pow(densematrix1, 2.0).sum() == sum(pow(bdensematrix1, 2.0)))
    //assert(abs(Ufuncs.pow(densematrix2, 2.0).sum() - sum(pow(bdensematrix2, 2.0f))) < 1.0)
  }

  @Test
  def sigmoidTest() {
    assert(TransFuncs.sigmoid(densematrix1).sum() == sum(sigmoid(bdensematrix1)))
    //assert(abs(TransFuncs.sigmoid(densematrix2).sum() - sum(sigmoid(bdensematrix2))) < 1.0)
  }

  @Test
  def softthresholdTest() {
    println(s"angel blasmatrix softthreshold : ${Ufuncs.softthreshold(densematrix1, 2.0).sum()}")
    println(s"angel blasmatrix softthreshold : ${Ufuncs.softthreshold(densematrix2, 2.0).sum()}")
  }

  @Test
  def sqrtTest() {
    assert(Ufuncs.sqrt(densematrix1).sum() == sum(sqrt(bdensematrix1)))
    //assert(abs(Ufuncs.sqrt(densematrix2).sum() - sum(sqrt(bdensematrix2))) < 1.0)
  }
}
