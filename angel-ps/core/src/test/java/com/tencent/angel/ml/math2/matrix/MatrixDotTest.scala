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


package com.tencent.angel.ml.math2.matrix

import java.util

import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.vector.Vector
import org.scalatest.FunSuite

class MatrixDotTest extends FunSuite {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 10
  val dim: Int = capacity * 10

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

  val densedoubleMatrixValues: Array[Double] = new Array[Double](300 * 10)
  val densefloatMatrixValues: Array[Float] = new Array[Float](300 * 10)

  val densedoubleMatrixValues1: Array[Double] = new Array[Double](10 * 300)
  val densefloatMatrixValues1: Array[Float] = new Array[Float](10 * 300)

  var densematrix1 = MFactory.denseDoubleMatrix(300, 10, densedoubleMatrixValues)
  var densematrix2 = MFactory.denseFloatMatrix(300, 10, densefloatMatrixValues)

  var densematrix3 = MFactory.denseDoubleMatrix(10, 300, densedoubleMatrixValues1)
  var densematrix4 = MFactory.denseFloatMatrix(10, 300, densefloatMatrixValues1)

  var intdummy = VFactory.intDummyVector(dim, intsortedIndices)
  var longdummy = VFactory.longDummyVector(dim, longsortedIndices)

  val matrixlist = new util.ArrayList[Matrix]()
  val vectorlist = new util.ArrayList[Vector]()

  val lmatrixlist = new util.ArrayList[Matrix]()
  val lvectorlist = new util.ArrayList[Vector]()


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


    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1_1 = Array(dense1, dense1, dense1)
    val list1_2 = Array(dense1, dense1, dense1)
    val list1_3 = Array(dense1, dense1, dense1)
    val list1_4 = Array(dense1, dense1, dense1)
    val list1_5 = Array(dense1, dense1, dense1)
    val list1_6 = Array(dense1, dense1, dense1)
    val list1_7 = Array(dense1, dense1, dense1)
    val list1_8 = Array(dense1, dense1, dense1)
    val list1_9 = Array(dense1, dense1, dense1)
    val list1_10 = Array(dense1, dense1, dense1)
    val comp1_1 = VFactory.compIntDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = VFactory.compIntDoubleVector(dim * list1_2.length, list1_2)
    val comp1_3 = VFactory.compIntDoubleVector(dim * list1_3.length, list1_3)
    val comp1_4 = VFactory.compIntDoubleVector(dim * list1_4.length, list1_4)
    val comp1_5 = VFactory.compIntDoubleVector(dim * list1_5.length, list1_5)
    val comp1_6 = VFactory.compIntDoubleVector(dim * list1_6.length, list1_6)
    val comp1_7 = VFactory.compIntDoubleVector(dim * list1_3.length, list1_3)
    val comp1_8 = VFactory.compIntDoubleVector(dim * list1_4.length, list1_4)
    val comp1_9 = VFactory.compIntDoubleVector(dim * list1_5.length, list1_5)
    val comp1_10 = VFactory.compIntDoubleVector(dim * list1_6.length, list1_6)


    matrixlist.add(MFactory.rbCompIntDoubleMatrix(Array(comp1_1, comp1_2, comp1_3, comp1_4, comp1_5, comp1_6, comp1_7, comp1_8, comp1_9, comp1_10)))

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2_1 = Array(dense2, dense2, dense2)
    val list2_2 = Array(dense2, dense2, dense2)
    val list2_3 = Array(dense2, dense2, dense2)
    val list2_4 = Array(dense2, dense2, dense2)
    val list2_5 = Array(dense2, dense2, dense2)
    val list2_6 = Array(dense2, dense2, dense2)
    val list2_7 = Array(dense2, dense2, dense2)
    val list2_8 = Array(dense2, dense2, dense2)
    val list2_9 = Array(dense2, dense2, dense2)
    val list2_10 = Array(dense2, dense2, dense2)

    val comp2_1 = VFactory.compIntFloatVector(dim * list2_1.length, list2_1)
    val comp2_2 = VFactory.compIntFloatVector(dim * list2_2.length, list2_2)
    val comp2_3 = VFactory.compIntFloatVector(dim * list2_3.length, list2_3)
    val comp2_4 = VFactory.compIntFloatVector(dim * list2_4.length, list2_4)
    val comp2_5 = VFactory.compIntFloatVector(dim * list2_5.length, list2_5)
    val comp2_6 = VFactory.compIntFloatVector(dim * list2_6.length, list2_6)
    val comp2_7 = VFactory.compIntFloatVector(dim * list2_7.length, list2_7)
    val comp2_8 = VFactory.compIntFloatVector(dim * list2_8.length, list2_8)
    val comp2_9 = VFactory.compIntFloatVector(dim * list2_9.length, list2_9)
    val comp2_10 = VFactory.compIntFloatVector(dim * list2_10.length, list2_10)


    matrixlist.add(MFactory.rbCompIntFloatMatrix(Array(comp2_1, comp2_2, comp2_3, comp2_4, comp2_5, comp2_6, comp2_7, comp2_8, comp2_9, comp2_10)))


    densematrix1 = MFactory.denseDoubleMatrix(300, 10, densedoubleMatrixValues)
    densematrix2 = MFactory.denseFloatMatrix(300, 10, densefloatMatrixValues)

    densematrix3 = MFactory.denseDoubleMatrix(10, 300, densedoubleMatrixValues1)
    densematrix4 = MFactory.denseFloatMatrix(10, 300, densefloatMatrixValues1)
  }

  test("rbmatrix dot blasmatrix") {
    init()
    println("rbmatrix dot blasmatrix FF")
    println(Ufuncs.dot(matrixlist.get(0), densematrix1).sum())
    println(Ufuncs.dot(matrixlist.get(1), densematrix2).sum())

    println("rbmatrix dot blasmatrix TF")
    println(Ufuncs.dot(matrixlist.get(0), true, densematrix3, false).sum())
    println(Ufuncs.dot(matrixlist.get(1), true, densematrix4, false).sum())

    println("rbmatrix dot blasmatrix FT")
    println(Ufuncs.dot(matrixlist.get(0), false, densematrix3, true).sum())
    println(Ufuncs.dot(matrixlist.get(1), false, densematrix4, true).sum())

    println("rbmatrix dot blasmatrix TT")
    println(Ufuncs.dot(matrixlist.get(0), true, densematrix1, true).sum())
    println(Ufuncs.dot(matrixlist.get(1), true, densematrix2, true).sum())

  }

  test("blasmatrix dot rbmatrix") {
    init()
    println("blasmatrix dot rbmatrix FF")
    println(Ufuncs.dot(densematrix1, matrixlist.get(0)).sum())
    println(Ufuncs.dot(densematrix2, matrixlist.get(1)).sum())

    println("blasmatrix dot rbmatrix TF")
    println(Ufuncs.dot(densematrix3, true, matrixlist.get(0), false).sum())
    println(Ufuncs.dot(densematrix4, true, matrixlist.get(1), false).sum())

    println("blasmatrix dot rbmatrix FT")
    println(Ufuncs.dot(densematrix3, false, matrixlist.get(0), true).sum())
    println(Ufuncs.dot(densematrix4, false, matrixlist.get(1), true).sum())

    println("blasmatrix dot rbmatrix TT")
    println(Ufuncs.dot(densematrix1, true, matrixlist.get(0), true).sum())
    println(Ufuncs.dot(densematrix2, true, matrixlist.get(1), true).sum())
  }
}
