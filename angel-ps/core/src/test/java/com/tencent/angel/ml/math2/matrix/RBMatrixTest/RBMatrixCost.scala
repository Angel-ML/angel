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

import breeze.collection.mutable.{OpenAddressHashArray, SparseArray}
import breeze.linalg.{CSCMatrix, DenseVector, HashVector, SparseVector, axpy, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.Vector
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object RBMatrixCost {
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

  val sparsedoubleValues = new Array[Double](dim)
  val sorteddoubleValues = new Array[Double](dim)
  val sparsefloatValues = new Array[Float](dim)
  val sortedfloatValues = new Array[Float](dim)
  val sparselongValues = new Array[Long](dim)
  val sortedlongValues = new Array[Long](dim)
  val sparseintValues = new Array[Int](dim)
  val sortedintValues = new Array[Int](dim)

  var bdoublematrix = CSCMatrix(densedoubleValues, sparsedoubleValues, sorteddoubleValues)
  var bfloatmatrix = CSCMatrix(densefloatValues, sparsefloatValues, sortedfloatValues)
  var blongmatrix = CSCMatrix(denselongValues, sparselongValues, sortedlongValues)
  var bintmatrix = CSCMatrix(denseintValues, sparseintValues, sortedintValues)

  var bdoublematrix1 = CSCMatrix(sorteddoubleValues, densedoubleValues, sparsedoubleValues)
  var bfloatmatrix1 = CSCMatrix(sortedfloatValues, densefloatValues, sparsefloatValues)
  var blongmatrix1 = CSCMatrix(sortedlongValues, denselongValues, sparselongValues)
  var bintmatrix1 = CSCMatrix(sortedintValues, denseintValues, sparseintValues)

  var dense1 = DenseVector[Double](densedoubleValues)
  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var dense2 = DenseVector[Float](densefloatValues)
  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var dense3 = DenseVector[Long](denselongValues)
  var sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var dense4 = DenseVector[Int](denseintValues)
  var sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

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
      doubleValues(i) = rand.nextDouble() + 0.01
    }

    floatValues.indices.foreach { i =>
      floatValues(i) = rand.nextFloat() + 0.01f
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(100) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(100) + 1
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

    var j = 0
    (0 until intrandIndices.length).foreach { i =>
      sparsedoubleValues(i) = doubleValues(j)
      j = j + 1
    }

    j = 0
    (0 until intsortedIndices.length).foreach { i =>
      sorteddoubleValues(i) = doubleValues(j)
      j = j + 1
    }

    j = 0
    (0 until intrandIndices.length).foreach { i =>
      sparsefloatValues(i) = floatValues(j)
      j = j + 1
    }

    j = 0
    (0 until intsortedIndices.length).foreach { i =>
      sortedfloatValues(i) = floatValues(j)
      j = j + 1
    }

    j = 0
    (0 until intrandIndices.length).foreach { i =>
      sparselongValues(i) = longValues(j)
      j = j + 1
    }

    j = 0
    (0 until intsortedIndices.length).foreach { i =>
      sortedlongValues(i) = longValues(j)
      j = j + 1
    }

    j = 0
    (0 until intrandIndices.length).foreach { i =>
      sparseintValues(i) = intValues(j)
      j = j + 1
    }

    j = 0
    (0 until intsortedIndices.length).foreach { i =>
      sortedintValues(i) = intValues(j)
      j = j + 1
    }

    bdoublematrix = CSCMatrix(densedoubleValues, sparsedoubleValues, sorteddoubleValues)
    bfloatmatrix = CSCMatrix(densefloatValues, sparsefloatValues, sortedfloatValues)
    blongmatrix = CSCMatrix(denselongValues, sparselongValues, sortedlongValues)
    bintmatrix = CSCMatrix(denseintValues, sparseintValues, sortedintValues)

    bdoublematrix1 = CSCMatrix(sorteddoubleValues, densedoubleValues, sparsedoubleValues)
    bfloatmatrix1 = CSCMatrix(sortedfloatValues, densefloatValues, sparsefloatValues)
    blongmatrix1 = CSCMatrix(sortedlongValues, denselongValues, sparselongValues)
    bintmatrix1 = CSCMatrix(sortedintValues, denseintValues, sparseintValues)

    dense1 = DenseVector[Double](densedoubleValues)
    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

    dense2 = DenseVector[Float](densefloatValues)
    sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse2(i) = v }
    sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

    dense3 = DenseVector[Long](denselongValues)
    sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse3(i) = v }
    sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

    dense4 = DenseVector[Int](denseintValues)
    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    val doubledense = VFactory.denseDoubleVector(densedoubleValues)
    val doublesparse = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val doublesorted = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)

    val floatdense = VFactory.denseFloatVector(densefloatValues)
    val floatsparse = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val floatsorted = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)

    val longdense = VFactory.denseLongVector(denselongValues)
    val longsparse = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val longsorted = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues)

    val intdense = VFactory.denseIntVector(denseintValues)
    val intsparse = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val intsorted = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)


    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doubledense, doublesparse, doublesorted)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatdense, floatsparse, floatsorted)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longdense, longsparse, longsorted)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intdense, intsparse, intsorted)))


    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doublesorted, doubledense, doublesparse)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatsorted, floatdense, floatsparse)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longsorted, longdense, longsparse)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intsorted, intdense, intsparse)))

    vectorlist.add(doubledense)
    vectorlist.add(doublesparse)
    vectorlist.add(doublesorted)
    vectorlist.add(floatdense)
    vectorlist.add(floatsparse)
    vectorlist.add(floatsorted)
    vectorlist.add(longdense)
    vectorlist.add(longsparse)
    vectorlist.add(longsorted)
    vectorlist.add(intdense)
    vectorlist.add(intsparse)
    vectorlist.add(intsorted)

    val ldoublesparse = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val ldoublesorted = VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues)

    val lfloatsparse = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lfloatsorted = VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues)

    val llongsparse = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val llongsorted = VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues)

    val lintsparse = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lintsorted = VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues)

    lmatrixlist.add(MFactory.rbLongDoubleMatrix(Array(ldoublesparse, ldoublesorted)))
    lmatrixlist.add(MFactory.rbLongFloatMatrix(Array(lfloatsparse, lfloatsorted)))
    lmatrixlist.add(MFactory.rbLongLongMatrix(Array(llongsparse, llongsorted)))
    lmatrixlist.add(MFactory.rbLongIntMatrix(Array(lintsparse, lintsorted)))

  }
}

class RBMatrixCost {
  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val matrixlist = RBMatrixCost.matrixlist
  val vectorlist = RBMatrixCost.vectorlist

  val lmatrixlist = RBMatrixCost.lmatrixlist
  val lvectorlist = RBMatrixCost.lvectorlist

  var bdoublematrix = RBMatrixCost.bdoublematrix
  var bfloatmatrix = RBMatrixCost.bfloatmatrix
  var blongmatrix = RBMatrixCost.blongmatrix
  var bintmatrix = RBMatrixCost.bintmatrix

  var bdoublematrix1 = RBMatrixCost.bdoublematrix1
  var bfloatmatrix1 = RBMatrixCost.bfloatmatrix1
  var blongmatrix1 = RBMatrixCost.blongmatrix1
  var bintmatrix1 = RBMatrixCost.bintmatrix1

  var dense1 = RBMatrixCost.dense1
  var sparse1 = RBMatrixCost.sparse1
  var sorted1 = RBMatrixCost.sorted1

  var dense2 = RBMatrixCost.dense2
  var sparse2 = RBMatrixCost.sparse2
  var sorted2 = RBMatrixCost.sorted2

  var dense3 = RBMatrixCost.dense3
  var sparse3 = RBMatrixCost.sparse3
  var sorted3 = RBMatrixCost.sorted3

  var dense4 = RBMatrixCost.dense4
  var sparse4 = RBMatrixCost.sparse4
  var sorted4 = RBMatrixCost.sorted4

  @Test
  def addTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).add(matrixlist.get(4))
      matrixlist.get(1).add(matrixlist.get(5))
      matrixlist.get(2).add(matrixlist.get(6))
      matrixlist.get(3).add(matrixlist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix + bdoublematrix1
      bfloatmatrix + bfloatmatrix1
      blongmatrix + blongmatrix1
      bintmatrix + bintmatrix1
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(matrixlist.get(0).add(matrixlist.get(4)).sum() - sum(bdoublematrix + bdoublematrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(1).add(matrixlist.get(5)).sum() - sum(bfloatmatrix + bfloatmatrix1)) < 1.0)
    assert(abs(matrixlist.get(2).add(matrixlist.get(6)).sum() - sum(blongmatrix + blongmatrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(3).add(matrixlist.get(7)).sum() - sum(bintmatrix + bintmatrix1)) < 1.0E-8)
  }

  @Test
  def subTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).sub(matrixlist.get(4))
      matrixlist.get(1).sub(matrixlist.get(5))
      matrixlist.get(2).sub(matrixlist.get(6))
      matrixlist.get(3).sub(matrixlist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix - bdoublematrix1
      bfloatmatrix - bfloatmatrix1
      blongmatrix - blongmatrix1
      bintmatrix - bintmatrix1
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(matrixlist.get(0).sub(matrixlist.get(4)).sum() - sum(bdoublematrix - bdoublematrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(1).sub(matrixlist.get(5)).sum() - sum(bfloatmatrix - bfloatmatrix1)) < 1.0E-4)
    assert(abs(matrixlist.get(2).sub(matrixlist.get(6)).sum() - sum(blongmatrix - blongmatrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(3).sub(matrixlist.get(7)).sum() - sum(bintmatrix - bintmatrix1)) < 1.0E-8)
  }

  @Test
  def mulTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).mul(matrixlist.get(4))
      matrixlist.get(1).mul(matrixlist.get(5))
      matrixlist.get(2).mul(matrixlist.get(6))
      matrixlist.get(3).mul(matrixlist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix :* bdoublematrix1
      bfloatmatrix :* bfloatmatrix1
      blongmatrix :* blongmatrix1
      bintmatrix :* bintmatrix1
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).mul(matrixlist.get(4)).sum() - (sum(dense1:* sorted1)+sum(sparse1:* dense1)+sum(sorted1:*sparse1)))<1.0E-8)
    //    assert(abs(matrixlist.get(1).mul(matrixlist.get(5)).sum() - (sum(dense2:* sorted2)+sum(sparse2:* dense2)+sum(sorted2:*sparse2)))<1.0E-4)
    //    assert(abs(matrixlist.get(2).mul(matrixlist.get(6)).sum() - (sum(dense3:* sorted3)+sum(sparse3:* dense3)+sum(sorted3:*sparse3)))<1.0E-8)
    //    assert(abs(matrixlist.get(3).mul(matrixlist.get(7)).sum() - (sum(dense4:* sorted4)+sum(sparse4:* dense4)+sum(sorted4:*sparse4)))<1.0E-8)
  }

  @Test
  def divTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).div(matrixlist.get(4))
      matrixlist.get(1).div(matrixlist.get(5))
      matrixlist.get(2).div(matrixlist.get(6))
      matrixlist.get(3).div(matrixlist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix :/ bdoublematrix1
      bfloatmatrix :/ bfloatmatrix1
      blongmatrix :/ blongmatrix1
      bintmatrix :/ bintmatrix1
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).div(matrixlist.get(4)).sum() - (sum(dense1:/ sorted1)+sum(sparse1:/ dense1)+sum(sorted1:/sparse1)))<1.0E-8)
    //    assert(abs(matrixlist.get(1).div(matrixlist.get(5)).sum() - (sum(dense2:/ sorted2)+sum(sparse2:/ dense2)+sum(sorted2:/sparse2)))<1.0E-4)
    //    assert(abs(matrixlist.get(2).div(matrixlist.get(6)).sum() - (sum(dense3:/ sorted3)+sum(sparse3:/ dense3)+sum(sorted3:/sparse3)))<1.0E-8)
    //    assert(abs(matrixlist.get(3).div(matrixlist.get(7)).sum() - (sum(dense4:/ sorted4)+sum(sparse4:/ dense4)+sum(sorted4:/sparse4)))<1.0E-8)

  }

  @Test
  def axpyTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).axpy(matrixlist.get(4), 2.0)
      matrixlist.get(1).axpy(matrixlist.get(5), 2.0)
      matrixlist.get(2).axpy(matrixlist.get(6), 2.0)
      matrixlist.get(3).axpy(matrixlist.get(7), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      axpy(2.0, bdoublematrix, bdoublematrix1)
      axpy(2.0f, bfloatmatrix, bfloatmatrix1)
      axpy(2L, blongmatrix, blongmatrix1)
      axpy(2, bintmatrix, bintmatrix1)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix axpy:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def dotMatrixTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.dot(matrixlist.get(0), matrixlist.get(4))
      Ufuncs.dot(matrixlist.get(1), matrixlist.get(5))
      Ufuncs.dot(matrixlist.get(2), matrixlist.get(6))
      Ufuncs.dot(matrixlist.get(3), matrixlist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix * bdoublematrix1
      bfloatmatrix * bfloatmatrix1
      blongmatrix * blongmatrix1
      bintmatrix * bintmatrix1
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(matrixlist.get(0).dot(matrixlist.get(4)).sum() - sum(bdoublematrix * bdoublematrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(1).dot(matrixlist.get(5)).sum() - sum(bfloatmatrix * bfloatmatrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(2).dot(matrixlist.get(6)).sum() - sum(blongmatrix * blongmatrix1)) < 1.0E-8)
    assert(abs(matrixlist.get(3).dot(matrixlist.get(7)).sum() - sum(bintmatrix * bintmatrix1)) < 1.0E-8)
  }

  @Test
  def dotVectorTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.dot(matrixlist.get(0), vectorlist.get(0))
      Ufuncs.dot(matrixlist.get(1), vectorlist.get(3))
      Ufuncs.dot(matrixlist.get(2), vectorlist.get(6))
      Ufuncs.dot(matrixlist.get(3), vectorlist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix * dense1
      bfloatmatrix * dense2
      blongmatrix * dense3
      bintmatrix * dense4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix dot dense vector :$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.dot(matrixlist.get(0),vectorlist.get(0)).sum() - (dense1.dot(dense1)+sparse1.dot(dense1)+sorted1.dot(dense1)))<1.0E-8)
    //    assert(abs(Ufuncs.dot(matrixlist.get(1),vectorlist.get(3)).sum() - (dense2.dot(dense2)+sparse2.dot(dense2)+sorted2.dot(dense2)))<1.0E-2)
    //    assert(abs(Ufuncs.dot(matrixlist.get(2),vectorlist.get(6)).sum() - (dense3.dot(dense3)+sparse3.dot(dense3)+sorted3.dot(dense3)))<1.0E-8)
    //    assert(abs(Ufuncs.dot(matrixlist.get(3),vectorlist.get(9)).sum() - (dense4.dot(dense4)+sparse4.dot(dense4)+sorted4.dot(dense4)))<1.0E-8)
  }


  @Test
  def saddTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).add(2.0)
      matrixlist.get(1).add(2.0)
      matrixlist.get(2).add(2.0)
      matrixlist.get(3).add(2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix + 2.0
      bfloatmatrix + 2.0f
      blongmatrix + 2L
      bintmatrix + 2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix sadd:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).add(2.0).sum() - sum(bdoublematrix + 2.0))<1.0E-8)
    //    assert(abs(matrixlist.get(1).add(2.0).sum() - sum(bfloatmatrix + 2.0f))<1.0)
    //    assert(abs(matrixlist.get(2).add(2.0).sum() - sum(blongmatrix + 2L))<1.0E-8)
    //    assert(abs(matrixlist.get(3).add(2.0).sum() - sum(bintmatrix + 2))<1.0E-8)
  }

  @Test
  def ssubTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).sub(2.0)
      matrixlist.get(1).sub(2.0)
      matrixlist.get(2).sub(2.0)
      matrixlist.get(3).sub(2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix - 2.0
      bfloatmatrix - 2.0f
      blongmatrix - 2L
      bintmatrix - 2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix ssub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).sub(2.0).sum() - sum(bdoublematrix - 2.0))<1.0E-8)
    //    assert(abs(matrixlist.get(1).sub(2.0).sum() - sum(bfloatmatrix - 2.0f))<1.0)
    //    assert(abs(matrixlist.get(2).sub(2.0).sum() - sum(blongmatrix - 2L))<1.0E-8)
    //    assert(abs(matrixlist.get(3).sub(2.0).sum() - sum(bintmatrix - 2))<1.0E-8)
  }

  @Test
  def smulTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).mul(2.0)
      matrixlist.get(1).mul(2.0)
      matrixlist.get(2).mul(2.0)
      matrixlist.get(3).mul(2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix :* 2.0
      bfloatmatrix :* 2.0f
      blongmatrix :* 2L
      bintmatrix :* 2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).mul(2.0).sum() - sum(bdoublematrix :* 2.0))<1.0E-8)
    //    assert(abs(matrixlist.get(1).mul(2.0).sum() - sum(bfloatmatrix :* 2.0f))<1.0)
    //    assert(abs(matrixlist.get(2).mul(2.0).sum() - sum(blongmatrix :* 2L))<1.0E-8)
    //    assert(abs(matrixlist.get(3).mul(2.0).sum() - sum(bintmatrix :* 2))<1.0E-8)
  }

  @Test
  def sdivTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      matrixlist.get(0).div(2.0)
      matrixlist.get(1).div(2.0)
      matrixlist.get(2).div(2.0)
      matrixlist.get(3).div(2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdoublematrix :/ 2.0
      bfloatmatrix :/ 2.0f
      blongmatrix :/ 2L
      bintmatrix :/ 2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(matrixlist.get(0).div(2.0).sum() - sum(bdoublematrix :/ 2.0))<1.0E-8)
    //    assert(abs(matrixlist.get(1).div(2.0).sum() - sum(bfloatmatrix :/ 2.0f))<1.0)
    //    assert(abs(matrixlist.get(2).div(2.0).sum() - sum(blongmatrix :/ 2L))<1.0E-8)
    //    assert(abs(matrixlist.get(3).div(2.0).sum() - sum(bintmatrix :/ 2))<1.0E-8)
  }

  @Test
  def expTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(matrixlist.get(0))
      Ufuncs.exp(matrixlist.get(1))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      exp(bdoublematrix)
      exp(bfloatmatrix)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix exp:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.exp(matrixlist.get(0)).sum() - sum(exp(bdoublematrix)))<1.0E-8)
    //    assert(abs(Ufuncs.exp(matrixlist.get(1)).sum() - sum(exp(bfloatmatrix)))<1.0)
  }

  @Test
  def logTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(matrixlist.get(0))
      Ufuncs.log(matrixlist.get(1))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log(bdoublematrix)
      log(bfloatmatrix)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix log:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(Ufuncs.log(matrixlist.get(0)).sum() == sum(log(bdoublematrix)))
    //    assert(Ufuncs.log(matrixlist.get(1)).sum() == sum(log(bfloatmatrix)))
  }

  @Test
  def log1pTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(matrixlist.get(0))
      Ufuncs.log1p(matrixlist.get(1))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log1p(bdoublematrix)
      log1p(bfloatmatrix)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix log1p:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.log1p(matrixlist.get(0)).sum() - sum(log1p(bdoublematrix)))<1.0E-8)
    //    assert(abs(Ufuncs.log1p(matrixlist.get(1)).sum() - sum(log1p(bfloatmatrix)))<1.0)
  }

  @Test
  def powTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(matrixlist.get(0), 2.0)
      Ufuncs.pow(matrixlist.get(1), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(bdoublematrix, 2.0)
      pow(bfloatmatrix, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.pow(matrixlist.get(0), 2.0).sum() - sum(pow(bdoublematrix, 2.0)))<1.0E-8)
    //    assert(abs(Ufuncs.pow(matrixlist.get(1), 2.0).sum() - sum(pow(bfloatmatrix, 2.0f)))<1.0)
  }

  @Test
  def sigmoidTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(matrixlist.get(0))
      TransFuncs.sigmoid(matrixlist.get(1))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sigmoid(bdoublematrix)
      sigmoid(bfloatmatrix)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix sigmoid:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.sigmoid(matrixlist.get(0)).sum() - sum(sigmoid(bdoublematrix)))<1.0E-8)
    //    assert(abs(Ufuncs.sigmoid(matrixlist.get(1)).sum() - sum(sigmoid(bfloatmatrix)))<1.0)
  }

  @Test
  def sqrtTest() {

    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(matrixlist.get(0))
      Ufuncs.sqrt(matrixlist.get(1))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(bdoublematrix)
      sqrt(bfloatmatrix)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparsematrix sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(abs(Ufuncs.sqrt(matrixlist.get(0)).sum() - sum(sqrt(bdoublematrix)))<1.0E-8)
    //    assert(abs(Ufuncs.sqrt(matrixlist.get(1)).sum() - sum(sqrt(bfloatmatrix)))<1.0)
  }
}
