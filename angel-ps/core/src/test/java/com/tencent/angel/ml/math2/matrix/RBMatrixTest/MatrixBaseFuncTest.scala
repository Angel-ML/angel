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


package com.tencent.angel.ml.math2.matrix.RBMatrixTest

import java.util

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}


object MatrixBaseFuncTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 1000
  val dim: Int = capacity * 100

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
  val vectorlist = new util.ArrayList[Vector]()

  val lmatrixlist = new util.ArrayList[Matrix]()
  val lvectorlist = new util.ArrayList[Vector]()

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
  }
}

class MatrixBaseFuncTest {
  val capacity: Int = MatrixBaseFuncTest.capacity
  val dim: Int = MatrixBaseFuncTest.dim

  val intrandIndices: Array[Int] = MatrixBaseFuncTest.intrandIndices
  val longrandIndices: Array[Long] = MatrixBaseFuncTest.longrandIndices
  val intsortedIndices: Array[Int] = MatrixBaseFuncTest.intsortedIndices
  val longsortedIndices: Array[Long] = MatrixBaseFuncTest.longsortedIndices

  val intValues: Array[Int] = MatrixBaseFuncTest.intValues
  val longValues: Array[Long] = MatrixBaseFuncTest.longValues
  val floatValues: Array[Float] = MatrixBaseFuncTest.floatValues
  val doubleValues: Array[Double] = MatrixBaseFuncTest.doubleValues

  val denseintValues: Array[Int] = MatrixBaseFuncTest.denseintValues
  val denselongValues: Array[Long] = MatrixBaseFuncTest.denselongValues
  val densefloatValues: Array[Float] = MatrixBaseFuncTest.densefloatValues
  val densedoubleValues: Array[Double] = MatrixBaseFuncTest.densedoubleValues

  @Test
  def RBIntDoubleMatrixTest() {
    val doubledense = VFactory.denseDoubleVector(densedoubleValues)
    val doublesparse = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val doublesorted = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)

    val dense = VFactory.denseDoubleVector(doubleValues)
    val sparse = VFactory.sparseDoubleVector(capacity, intrandIndices, doubleValues)
    val sorted = VFactory.sortedDoubleVector(capacity, intsortedIndices, doubleValues)

    val doubleMatrix = new RBIntDoubleMatrix(Array(doubledense, doublesparse, doublesorted))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, doublesorted)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(doublesorted, doubledense))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(dense, sparse, sorted))
    } catch {
      case e: AssertionError => {
        println(e)
      }
    }
  }

  @Test
  def RBIntFloatMatrixTest() {
    val floatdense = VFactory.denseFloatVector(densefloatValues)
    val floatsparse = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val floatsorted = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)

    val dense = VFactory.denseFloatVector(floatValues)
    val sparse = VFactory.sparseFloatVector(capacity, intrandIndices, floatValues)
    val sorted = VFactory.sortedFloatVector(capacity, intsortedIndices, floatValues)

    val floatMatrix = new RBIntFloatMatrix(Array(floatdense, floatsparse, floatsorted))
    val x = floatMatrix.get(0, 0)

    floatMatrix.set(0, 0, 10)
    println(x, floatMatrix.get(0, 0), floatMatrix.sum())

    floatMatrix.setRow(0, floatsorted)
    println(floatMatrix.sum())

    floatMatrix.setRows(Array(floatsorted, floatdense))
    println(floatMatrix.sum())

    try {
      floatMatrix.setRows(Array(dense, sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBIntLongMatrixTest() {
    val longdense = VFactory.denseLongVector(denselongValues)
    val longsparse = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val longsorted = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues)

    val dense = VFactory.denseLongVector(longValues)
    val sparse = VFactory.sparseLongVector(capacity, intrandIndices, longValues)
    val sorted = VFactory.sortedLongVector(capacity, intsortedIndices, longValues)

    val longMatrix = new RBIntLongMatrix(Array(longdense, longsparse, longsorted))
    val x = longMatrix.get(0, 0)

    longMatrix.set(0, 0, 10)
    println(x, longMatrix.get(0, 0), longMatrix.sum())

    longMatrix.setRow(0, longsorted)
    println(longMatrix.sum())

    longMatrix.setRows(Array(longsorted, longdense))
    println(longMatrix.sum())

    try {
      longMatrix.setRows(Array(dense, sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBIntIntMatrixTest() {
    val intdense = VFactory.denseIntVector(denseintValues)
    val intsparse = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val intsorted = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)

    val dense = VFactory.denseIntVector(intValues)
    val sparse = VFactory.sparseIntVector(capacity, intrandIndices, intValues)
    val sorted = VFactory.sortedIntVector(capacity, intsortedIndices, intValues)

    val intMatrix = new RBIntIntMatrix(Array(intdense, intsparse, intsorted))
    val x = intMatrix.get(0, 0)

    intMatrix.set(0, 0, 10)
    println(x, intMatrix.get(0, 0), intMatrix.sum())

    intMatrix.setRow(0, intsorted)
    println(intMatrix.sum())

    intMatrix.setRows(Array(intsorted, intsparse, intdense))
    println(intMatrix.sum())

    try {
      intMatrix.setRows(Array(dense, sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBLongDoubleMatrixTest() {
    val ldoublesparse = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val ldoublesorted = VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues)

    val sparse = VFactory.sparseLongKeyDoubleVector(capacity, longrandIndices, doubleValues)
    val sorted = VFactory.sortedLongKeyDoubleVector(capacity, longsortedIndices, densedoubleValues)

    val doubleMatrix = new RBLongDoubleMatrix(Array(ldoublesparse, ldoublesorted))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, ldoublesorted)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(ldoublesorted, ldoublesparse))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBLongFloatMatrixTest() {
    val lfloatsparse = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lfloatsorted = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)

    val sparse = VFactory.sparseLongKeyFloatVector(capacity, longrandIndices, floatValues)
    val sorted = VFactory.sortedLongKeyFloatVector(capacity, longsortedIndices, floatValues)

    val floatMatrix = new RBLongFloatMatrix(Array(lfloatsparse, lfloatsorted))
    val x = floatMatrix.get(0, 0)

    floatMatrix.set(0, 0, 10)
    println(x, floatMatrix.get(0, 0), floatMatrix.sum())

    floatMatrix.setRow(0, lfloatsorted)
    println(floatMatrix.sum())

    floatMatrix.setRows(Array(lfloatsorted, lfloatsparse))
    println(floatMatrix.sum())

    try {
      floatMatrix.setRows(Array(sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBLongLongMatrixTest() {
    val llongsparse = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val llongsorted = VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues)

    val sparse = VFactory.sparseLongKeyLongVector(capacity, longrandIndices, longValues)
    val sorted = VFactory.sortedLongKeyLongVector(capacity, longsortedIndices, longValues)

    val longMatrix = new RBLongLongMatrix(Array(llongsparse, llongsorted))
    val x = longMatrix.get(0, 0)

    longMatrix.set(0, 0, 10)
    println(x, longMatrix.get(0, 0), longMatrix.sum())

    longMatrix.setRow(0, llongsorted)
    println(longMatrix.sum())

    longMatrix.setRows(Array(llongsorted, llongsparse))
    println(longMatrix.sum())

    try {
      longMatrix.setRows(Array(sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBLongIntMatrixTest() {
    val lintsparse = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lintsorted = VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues)

    val sparse = VFactory.sparseLongKeyIntVector(capacity, longrandIndices, intValues)
    val sorted = VFactory.sortedLongKeyIntVector(capacity, longsortedIndices, intValues)

    val intMatrix = new RBLongIntMatrix(Array(lintsparse, lintsorted))
    val x = intMatrix.get(0, 0)

    intMatrix.set(0, 0, 10)
    println(x, intMatrix.get(0, 0), intMatrix.sum())

    intMatrix.setRow(0, lintsorted)
    println(intMatrix.sum())

    intMatrix.setRows(Array(lintsorted, lintsparse))
    println(intMatrix.sum())

    try {
      intMatrix.setRows(Array(sparse, sorted))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }


  @Test
  def RBCompIntDoubleMatrixTest() {
    val doubledense = VFactory.denseDoubleVector(densedoubleValues)
    val doublesparse = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val doublesorted = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
    val list1_1 = Array(doubledense, doublesparse, doublesorted)
    val list1_2 = Array(doublesparse, doubledense, doublesorted)
    val list1_3 = Array(doublesparse, doublesorted, doubledense)
    val comp1_1 = new CompIntDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompIntDoubleVector(dim * list1_2.length, list1_2)
    val comp1_3 = new CompIntDoubleVector(dim * list1_3.length, list1_3)

    val dense = VFactory.denseDoubleVector(doubleValues)
    val sparse = VFactory.sparseDoubleVector(capacity, intrandIndices, doubleValues)
    val sorted = VFactory.sortedDoubleVector(capacity, intsortedIndices, doubleValues)
    val list1 = Array(dense, sparse, sorted)
    val list2 = Array(sparse, dense, sorted)
    val list3 = Array(sorted, sparse, dense)
    val comp1 = new CompIntDoubleVector(capacity * list1.length, list1)
    val comp2 = new CompIntDoubleVector(capacity * list2.length, list2)
    val comp3 = new CompIntDoubleVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompIntDoubleMatrix(Array(comp1_1, comp1_2, comp1_3))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_3)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_3))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2, comp3))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompIntFloatMatrixTest() {
    val floatdense = VFactory.denseFloatVector(densefloatValues)
    val floatsparse = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val floatsorted = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
    val list1_1 = Array(floatdense, floatsparse, floatsorted)
    val list1_2 = Array(floatsparse, floatdense, floatsorted)
    val list1_3 = Array(floatsparse, floatsorted, floatdense)
    val comp1_1 = new CompIntFloatVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompIntFloatVector(dim * list1_2.length, list1_2)
    val comp1_3 = new CompIntFloatVector(dim * list1_3.length, list1_3)

    val dense = VFactory.denseFloatVector(floatValues)
    val sparse = VFactory.sparseFloatVector(capacity, intrandIndices, floatValues)
    val sorted = VFactory.sortedFloatVector(capacity, intsortedIndices, floatValues)
    val list1 = Array(dense, sparse, sorted)
    val list2 = Array(sparse, dense, sorted)
    val list3 = Array(sorted, sparse, dense)
    val comp1 = new CompIntFloatVector(capacity * list1.length, list1)
    val comp2 = new CompIntFloatVector(capacity * list2.length, list2)
    val comp3 = new CompIntFloatVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompIntFloatMatrix(Array(comp1_1, comp1_2, comp1_3))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_3)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_3))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2, comp3))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompIntLongMatrixTest() {
    val longdense = VFactory.denseLongVector(denselongValues)
    val longsparse = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val longsorted = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues)
    val list1_1 = Array(longdense, longsparse, longsorted)
    val list1_2 = Array(longsparse, longdense, longsorted)
    val list1_3 = Array(longsparse, longsorted, longdense)
    val comp1_1 = new CompIntLongVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompIntLongVector(dim * list1_2.length, list1_2)
    val comp1_3 = new CompIntLongVector(dim * list1_3.length, list1_3)

    val dense = VFactory.denseLongVector(longValues)
    val sparse = VFactory.sparseLongVector(capacity, intrandIndices, longValues)
    val sorted = VFactory.sortedLongVector(capacity, intsortedIndices, longValues)
    val list1 = Array(dense, sparse, sorted)
    val list2 = Array(sparse, dense, sorted)
    val list3 = Array(sorted, sparse, dense)
    val comp1 = new CompIntLongVector(capacity * list1.length, list1)
    val comp2 = new CompIntLongVector(capacity * list2.length, list2)
    val comp3 = new CompIntLongVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompIntLongMatrix(Array(comp1_1, comp1_2, comp1_3))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_3)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_3))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2, comp3))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompIntIntMatrixTest() {
    val intdense = VFactory.denseIntVector(denseintValues)
    val intesparse = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val intsorted = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)
    val list1_1 = Array(intdense, intesparse, intsorted)
    val list1_2 = Array(intesparse, intdense, intsorted)
    val list1_3 = Array(intesparse, intsorted, intdense)
    val comp1_1 = new CompIntIntVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompIntIntVector(dim * list1_2.length, list1_2)
    val comp1_3 = new CompIntIntVector(dim * list1_3.length, list1_3)

    val dense = VFactory.denseIntVector(intValues)
    val sparse = VFactory.sparseIntVector(capacity, intrandIndices, intValues)
    val sorted = VFactory.sortedIntVector(capacity, intsortedIndices, intValues)
    val list1 = Array(dense, sparse, sorted)
    val list2 = Array(sparse, dense, sorted)
    val list3 = Array(sorted, sparse, dense)
    val comp1 = new CompIntIntVector(capacity * list1.length, list1)
    val comp2 = new CompIntIntVector(capacity * list2.length, list2)
    val comp3 = new CompIntIntVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompIntIntMatrix(Array(comp1_1, comp1_2, comp1_3))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_3)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_3))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2, comp3))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompLongDoubleMatrixTest() {
    val doublesparse = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val doublesorted = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1_1 = Array(doublesorted, doublesparse)
    val list1_2 = Array(doublesparse, doublesorted)
    val comp1_1 = new CompLongDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompLongDoubleVector(dim * list1_2.length, list1_2)


    val sparse = VFactory.sparseLongKeyDoubleVector(capacity, longrandIndices, doubleValues)
    val sorted = VFactory.sortedLongKeyDoubleVector(capacity, longsortedIndices, doubleValues)
    val list2 = Array(sparse, sorted)
    val list3 = Array(sorted, sparse)
    val comp1 = new CompLongDoubleVector(capacity * list2.length, list2)
    val comp2 = new CompLongDoubleVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompLongDoubleMatrix(Array(comp1_1, comp1_2))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_2)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_1))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompLongFloatMatrixTest() {
    val floatsparse = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val floatsorted = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list1_1 = Array(floatsorted, floatsparse)
    val list1_2 = Array(floatsparse, floatsorted)
    val comp1_1 = new CompLongFloatVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompLongFloatVector(dim * list1_2.length, list1_2)


    val sparse = VFactory.sparseLongKeyFloatVector(capacity, longrandIndices, floatValues)
    val sorted = VFactory.sortedLongKeyFloatVector(capacity, longsortedIndices, floatValues)
    val list2 = Array(sparse, sorted)
    val list3 = Array(sorted, sparse)
    val comp1 = new CompLongFloatVector(capacity * list2.length, list2)
    val comp2 = new CompLongFloatVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompLongFloatMatrix(Array(comp1_1, comp1_2))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_2)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_1))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompLongLongMatrixTest() {
    val longsparse = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val longsorted = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list1_1 = Array(longsparse, longsorted)
    val list1_2 = Array(longsorted, longsparse)
    val comp1_1 = new CompLongLongVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompLongLongVector(dim * list1_2.length, list1_2)


    val sparse = VFactory.sparseLongKeyLongVector(capacity, longrandIndices, longValues)
    val sorted = VFactory.sortedLongKeyLongVector(capacity, longsortedIndices, longValues)
    val list2 = Array(sparse, sorted)
    val list3 = Array(sorted, sparse)
    val comp1 = new CompLongLongVector(capacity * list2.length, list2)
    val comp2 = new CompLongLongVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompLongLongMatrix(Array(comp1_1, comp1_2))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_2)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_1))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }

  @Test
  def RBCompLongIntMatrixTest() {
    val intsparse = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val intsorted = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list1_1 = Array(intsparse, intsorted)
    val list1_2 = Array(intsorted, intsparse)
    val comp1_1 = new CompLongIntVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompLongIntVector(dim * list1_2.length, list1_2)


    val sparse = VFactory.sparseLongKeyIntVector(capacity, longrandIndices, intValues)
    val sorted = VFactory.sortedLongKeyIntVector(capacity, longsortedIndices, intValues)
    val list2 = Array(sparse, sorted)
    val list3 = Array(sorted, sparse)
    val comp1 = new CompLongIntVector(capacity * list2.length, list2)
    val comp2 = new CompLongIntVector(capacity * list3.length, list3)

    val doubleMatrix = new RBCompLongIntMatrix(Array(comp1_1, comp1_2))
    val x = doubleMatrix.get(0, 0)

    doubleMatrix.set(0, 0, 10)
    println(x, doubleMatrix.get(0, 0), doubleMatrix.sum())

    doubleMatrix.setRow(0, comp1_2)
    println(doubleMatrix.sum())

    doubleMatrix.setRows(Array(comp1_2, comp1_1))
    println(doubleMatrix.sum())

    try {
      doubleMatrix.setRows(Array(comp1, comp2))
    } catch {
      case e: AssertionError => {
        e
      }
    }
  }
}
