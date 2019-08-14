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

import breeze.linalg.DenseMatrix
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MathException
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object BlasMatrixBinaryTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 10
  val dim: Int = capacity * 100

  val dim1: Int = 100

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

  val intrandIndices1: Array[Int] = new Array[Int](capacity)
  val longrandIndices1: Array[Long] = new Array[Long](capacity)
  val intsortedIndices1: Array[Int] = new Array[Int](capacity)
  val longsortedIndices1: Array[Long] = new Array[Long](capacity)

  val denseintValues1: Array[Int] = new Array[Int](dim1)
  val denselongValues1: Array[Long] = new Array[Long](dim1)
  val densefloatValues1: Array[Float] = new Array[Float](dim1)
  val densedoubleValues1: Array[Double] = new Array[Double](dim1)

  val densedoubleMatrixValues: Array[Double] = new Array[Double](100 * 1000)
  val densefloatMatrixValues: Array[Float] = new Array[Float](100 * 1000)

  val densedoubleMatrixValues1: Array[Double] = new Array[Double](100 * 100)
  val densefloatMatrixValues1: Array[Float] = new Array[Float](100 * 100)

  val matrixlist = new util.ArrayList[Matrix]()
  val matrixlist1 = new util.ArrayList[Matrix]()
  val ilist = new util.ArrayList[Vector]()
  val ilist1 = new util.ArrayList[Vector]()


  var densematrix1 = MFactory.denseDoubleMatrix(100, 1000, densedoubleMatrixValues)
  var densematrix2 = MFactory.denseFloatMatrix(100, 1000, densefloatMatrixValues)

  var bdensematrix1 = new DenseMatrix[Double](100, 1000, densedoubleMatrixValues)
  var bdensematrix2 = new DenseMatrix[Float](100, 1000, densefloatMatrixValues)

  var densematrix3 = MFactory.denseDoubleMatrix(100, 100, densedoubleMatrixValues1)
  var densematrix4 = MFactory.denseFloatMatrix(100, 100, densefloatMatrixValues1)

  var bdensematrix3 = new DenseMatrix[Double](100, 100, densedoubleMatrixValues1)
  var bdensematrix4 = new DenseMatrix[Float](100, 100, densefloatMatrixValues1)


  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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

    //another data
    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim1)
      if (!set.contains(t)) {
        intrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim1)
      if (!set.contains(t)) {
        longrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(intrandIndices1, 0, intsortedIndices1, 0, capacity)
    util.Arrays.sort(intsortedIndices1)

    System.arraycopy(longrandIndices1, 0, longsortedIndices1, 0, capacity)
    util.Arrays.sort(longsortedIndices1)

    densedoubleValues1.indices.foreach { j =>
      densedoubleValues1(j) = rand.nextDouble() + 0.01
    }

    densefloatValues1.indices.foreach { i =>
      densefloatValues1(i) = rand.nextFloat() + 0.01f
    }

    denselongValues1.indices.foreach { i =>
      denselongValues1(i) = rand.nextInt(100) + 1L
    }

    denseintValues1.indices.foreach { i =>
      denseintValues1(i) = rand.nextInt(100) + 1
    }

    densedoubleMatrixValues.indices.foreach { j =>
      densedoubleMatrixValues(j) = rand.nextDouble() + 0.01
    }

    densefloatMatrixValues.indices.foreach { i =>
      densefloatMatrixValues(i) = rand.nextFloat() + 0.01f
    }

    densedoubleMatrixValues1.indices.foreach { j =>
      densedoubleMatrixValues1(j) = rand.nextDouble() + 0.01
    }

    densefloatMatrixValues1.indices.foreach { i =>
      densefloatMatrixValues1(i) = rand.nextFloat() + 0.01f
    }

    densematrix1 = MFactory.denseDoubleMatrix(100, 1000, densedoubleMatrixValues)
    densematrix2 = MFactory.denseFloatMatrix(100, 1000, densefloatMatrixValues)

    matrixlist.add(densematrix1)
    matrixlist.add(densematrix2)

    densematrix3 = MFactory.denseDoubleMatrix(100, 100, densedoubleMatrixValues1)
    densematrix4 = MFactory.denseFloatMatrix(100, 100, densefloatMatrixValues1)

    matrixlist1.add(densematrix3)
    matrixlist1.add(densematrix4)

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))
    ilist.add(VFactory.denseFloatVector(densefloatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    ilist.add(VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues))
    ilist.add(VFactory.denseLongVector(denselongValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    ilist.add(VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues))
    ilist.add(VFactory.denseIntVector(denseintValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    ilist.add(VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues))
    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))


    ilist1.add(VFactory.denseDoubleVector(densedoubleValues1))
    ilist1.add(VFactory.sparseDoubleVector(dim1, intrandIndices1, doubleValues))
    ilist1.add(VFactory.sortedDoubleVector(dim1, intsortedIndices1, doubleValues))
    ilist1.add(VFactory.denseFloatVector(densefloatValues1))
    ilist1.add(VFactory.sparseFloatVector(dim1, intrandIndices1, floatValues))
    ilist1.add(VFactory.sortedFloatVector(dim1, intsortedIndices1, floatValues))
    ilist1.add(VFactory.denseLongVector(denselongValues1))
    ilist1.add(VFactory.sparseLongVector(dim1, intrandIndices1, longValues))
    ilist1.add(VFactory.sortedLongVector(dim1, intsortedIndices1, longValues))
    ilist1.add(VFactory.denseIntVector(denseintValues1))
    ilist1.add(VFactory.sparseIntVector(dim1, intrandIndices1, intValues))
    ilist1.add(VFactory.sortedIntVector(dim1, intsortedIndices1, intValues))
    ilist1.add(VFactory.intDummyVector(dim1, intsortedIndices1))
  }
}

class BlasMatrixBinaryTest {
  val matrixlist = BlasMatrixBinaryTest.matrixlist
  val matrixlist1 = BlasMatrixBinaryTest.matrixlist1
  val ilist = BlasMatrixBinaryTest.ilist
  val ilist1 = BlasMatrixBinaryTest.ilist1

  var densematrix1 = BlasMatrixBinaryTest.densematrix1
  var densematrix2 = BlasMatrixBinaryTest.densematrix2

  var bdensematrix1 = BlasMatrixBinaryTest.bdensematrix1
  var bdensematrix2 = BlasMatrixBinaryTest.bdensematrix2

  var densematrix3 = BlasMatrixBinaryTest.densematrix3
  var densematrix4 = BlasMatrixBinaryTest.densematrix4

  var bdensematrix3 = BlasMatrixBinaryTest.bdensematrix3
  var bdensematrix4 = BlasMatrixBinaryTest.bdensematrix4

  val densedoubleMatrixValues: Array[Double] = BlasMatrixBinaryTest.densedoubleMatrixValues
  val densefloatMatrixValues: Array[Float] = BlasMatrixBinaryTest.densefloatMatrixValues

  val densedoubleMatrixValues1: Array[Double] = BlasMatrixBinaryTest.densedoubleMatrixValues1
  val densefloatMatrixValues1: Array[Float] = BlasMatrixBinaryTest.densefloatMatrixValues1


  @Test
  def addTest() {

    //blasmatrix vs vector
    (0 until matrixlist1.size()).foreach { i =>
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix add vector: ${matrixlist1.get(i).add(ilist1.get(j)).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }

      //blasmatrix vs blasmatrix
      (0 until matrixlist1.size()).foreach { j =>
        try {
          println(s"blasmatrix add blasmatrix : ${matrixlist1.get(i).add(matrixlist1.get(j)).sum()}")
          println(s"blasmatrix add blasmatrix : ${Ufuncs.add(matrixlist1.get(i), matrixlist1.get(j)).sum()}")
          println(s"blasmatrix add blasmatrix : ${Ufuncs.add(matrixlist1.get(i), true, matrixlist1.get(j), true).sum()}")
          println(s"blasmatrix add blasmatrix : ${Ufuncs.add(matrixlist1.get(i), true, matrixlist1.get(j), false).sum()}")
          println(s"blasmatrix add blasmatrix : ${Ufuncs.add(matrixlist1.get(i), false, matrixlist1.get(j), true).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      println(s"blasmatrix add numeric : ${matrixlist1.get(i).add(2.0).sum()}")
    }
  }

  @Test
  def subTest() {

    //blasmatrix vs vector
    (0 until matrixlist1.size()).foreach { i =>
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix sub vector: ${matrixlist1.get(i).sub(ilist1.get(j)).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }

      //blasmatrix vs blasmatrix
      (0 until matrixlist1.size()).foreach { j =>
        try {
          println(s"blasmatrix sub blasmatrix : ${matrixlist1.get(i).sub(matrixlist1.get(j)).sum()}")
          println(s"blasmatrix sub blasmatrix : ${Ufuncs.sub(matrixlist1.get(i), matrixlist1.get(j)).sum()}")
          println(s"blasmatrix sub blasmatrix : ${Ufuncs.sub(matrixlist1.get(i), true, matrixlist1.get(j), true).sum()}")
          println(s"blasmatrix sub blasmatrix : ${Ufuncs.sub(matrixlist1.get(i), true, matrixlist1.get(j), false).sum()}")
          println(s"blasmatrix sub blasmatrix : ${Ufuncs.sub(matrixlist1.get(i), false, matrixlist1.get(j), true).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      println(s"blasmatrix sub numeric : ${matrixlist1.get(i).sub(2.0).sum()}")
    }
  }

  @Test
  def mulTest() {

    //blasmatrix vs vector
    (0 until matrixlist1.size()).foreach { i =>
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix mul vector: ${matrixlist1.get(i).mul(ilist1.get(j)).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }

      //blasmatrix vs blasmatrix
      (0 until matrixlist1.size()).foreach { j =>
        try {
          println(s"blasmatrix mul blasmatrix : ${matrixlist1.get(i).mul(matrixlist1.get(j)).sum()}")
          println(s"blasmatrix mul blasmatrix : ${Ufuncs.mul(matrixlist1.get(i), matrixlist1.get(j)).sum()}")
          println(s"blasmatrix mul blasmatrix : ${Ufuncs.mul(matrixlist1.get(i), true, matrixlist1.get(j), true).sum()}")
          println(s"blasmatrix mul blasmatrix : ${Ufuncs.mul(matrixlist1.get(i), true, matrixlist1.get(j), false).sum()}")
          println(s"blasmatrix mul blasmatrix : ${Ufuncs.mul(matrixlist1.get(i), false, matrixlist1.get(j), true).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      println(s"blasmatrix mul numeric : ${matrixlist1.get(i).mul(2.0).sum()}")
    }
  }

  @Test
  def divTest() {

    //blasmatrix vs vector
    (0 until matrixlist1.size()).foreach { i =>
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix div vector: ${matrixlist1.get(i).div(ilist1.get(j)).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }

      //blasmatrix vs blasmatrix
      (0 until matrixlist1.size()).foreach { j =>
        try {
          println(s"blasmatrix div blasmatrix : ${matrixlist1.get(i).div(matrixlist1.get(j)).sum()}")
          println(s"blasmatrix div blasmatrix : ${Ufuncs.div(matrixlist1.get(i), matrixlist1.get(j)).sum()}")
          println(s"blasmatrix div blasmatrix : ${Ufuncs.div(matrixlist1.get(i), true, matrixlist1.get(j), true).sum()}")
          println(s"blasmatrix div blasmatrix : ${Ufuncs.div(matrixlist1.get(i), true, matrixlist1.get(j), false).sum()}")
          println(s"blasmatrix div blasmatrix : ${Ufuncs.div(matrixlist1.get(i), false, matrixlist1.get(j), true).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      println(s"blasmatrix div numeric : ${matrixlist1.get(i).div(2.0).sum()}")
    }
  }

  @Test
  def axpyTest() {

    //blasmatrix vs vector
    (0 until matrixlist1.size()).foreach { i =>
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix axpy vector: ${matrixlist1.get(i).axpy(ilist1.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }

      //blasmatrix vs blasmatrix
      (0 until matrixlist.size()).foreach { j =>
        try {
          println(s"blasmatrix axpy blasmatrix : ${matrixlist1.get(i).axpy(matrixlist1.get(j), 2.0).sum()}")
          println(s"blasmatrix axpy blasmatrix : ${Ufuncs.axpy(matrixlist1.get(i), matrixlist1.get(j), 2.0).sum()}")
          println(s"blasmatrix axpy blasmatrix : ${Ufuncs.axpy(matrixlist1.get(i), true, matrixlist1.get(j), true, 2.0).sum()}")
          println(s"blasmatrix axpy blasmatrix : ${Ufuncs.axpy(matrixlist1.get(i), false, matrixlist1.get(j), true, 2.0).sum()}")
          println(s"blasmatrix axpy blasmatrix : ${Ufuncs.axpy(matrixlist1.get(i), true, matrixlist1.get(j), false, 2.0).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def dotTest() {

    (0 until matrixlist.size()).foreach { i =>
      //blasmatrix vs vector
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"blasmatrix dot vector: ${Ufuncs.dot(matrixlist.get(i), ilist.get(j)).sum()}")
          matrixlist.get(i).dot(ilist.get(j))
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix dot vector: ${Ufuncs.dot(matrixlist1.get(i), true, ilist1.get(j)).sum()}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
    //blasmatrix vs blasmatrix
    matrixlist1.get(0).dot(matrixlist1.get(0), true)
    matrixlist1.get(1).dot(matrixlist1.get(1),true)
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix3, densematrix1, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix4, densematrix2, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix3, true, densematrix3, true, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix4, true, densematrix4, true, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix3, true, densematrix3, false, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix4, true, densematrix4, false, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix3, false, densematrix3, true, true).sum()}")
    println(s"blasmatrix dot blasmatrix : ${Ufuncs.dot(densematrix4, false, densematrix4, true, true).sum()}")
  }

  @Test
  def xAxTest() {

    (0 until matrixlist1.size()).foreach { i =>
      //blasmatrix vs vector
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix dot vector: ${Ufuncs.xAx(matrixlist1.get(i), ilist1.get(j))}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def xAyTest() {

    (0 until matrixlist.size()).foreach { i =>
      //blasmatrix vs vector
      (0 until ilist1.size()).foreach { j =>
        try {
          println(s"blasmatrix dot vector: ${Ufuncs.xAy(matrixlist.get(i), ilist1.get(j), ilist.get(j))}")
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def rank1updateTest() {

    (0 until matrixlist.size()).foreach { i =>
      //blasmatrix vs vector
      (0 until ilist1.size()).foreach { j =>
        (0 until ilist.size()).foreach { t =>
          try {
            println(s"blasmatrix dot vector: ${Ufuncs.rank1update(matrixlist.get(i), 0.5, ilist1.get(j), ilist.get(t)).sum()}")
          } catch {
            case e: AngelException => e
            case e: MathException => e
          }
        }
      }
    }
  }

}
