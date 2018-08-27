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

import breeze.collection.mutable.{OpenAddressHashArray, SparseArray}
import com.tencent.angel.ml.math2.vector.Vector
import breeze.linalg.{DenseMatrix, DenseVector, HashVector, SparseVector, axpy, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import org.scalatest.FunSuite

class BlasMatrixCost extends FunSuite {
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

  val densematrixfloatValues: Array[Float] = new Array[Float](100 * 100)
  val densematrixdoubleValues: Array[Double] = new Array[Double](100 * 100)


  val matrixlist = new util.ArrayList[Matrix]()
  val ilist = new util.ArrayList[Vector]()
  var densematrix1 = MFactory.denseDoubleMatrix(100, 100, densematrixdoubleValues)
  var densematrix2 = MFactory.denseFloatMatrix(100, 100, densematrixfloatValues)

  var bdensematrix1 = new DenseMatrix[Double](100, 100, densematrixdoubleValues)
  var bdensematrix2 = new DenseMatrix[Float](100, 100, densematrixfloatValues)


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

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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

    densematrixdoubleValues.indices.foreach { j =>
      densematrixdoubleValues(j) = rand.nextDouble() + 0.01
    }

    densematrixfloatValues.indices.foreach { i =>
      densematrixfloatValues(i) = rand.nextFloat() + 0.01f
    }

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

    densematrix1 = MFactory.denseDoubleMatrix(100, 100, densematrixdoubleValues)
    densematrix2 = MFactory.denseFloatMatrix(100, 100, densematrixfloatValues)

    bdensematrix1 = new DenseMatrix[Double](100, 100, densematrixdoubleValues)
    bdensematrix2 = new DenseMatrix[Float](100, 100, densematrixfloatValues)

  }


  test("binary add test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.add(densematrix1, densematrix1)
      Ufuncs.add(densematrix1, true, densematrix1, true)
      Ufuncs.add(densematrix1, true, densematrix1, false)
      Ufuncs.add(densematrix1, false, densematrix1, true)
      Ufuncs.add(densematrix2, densematrix2)
      Ufuncs.add(densematrix2, true, densematrix2, true)
      Ufuncs.add(densematrix2, true, densematrix2, false)
      Ufuncs.add(densematrix2, false, densematrix2, true)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t + bdensematrix1.t
      bdensematrix1 + bdensematrix1
      bdensematrix1.t + bdensematrix1
      bdensematrix1 + bdensematrix1.t
      bdensematrix2.t + bdensematrix2.t
      bdensematrix2 + bdensematrix2
      bdensematrix2 + bdensematrix2.t
      bdensematrix2.t + bdensematrix2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.add(densematrix1, densematrix1).sum() - sum(bdensematrix1.t + bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.add(densematrix1, true, densematrix1, true).sum() - sum(bdensematrix1 + bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.add(densematrix1, true, densematrix1, false).sum() - sum(bdensematrix1 + bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.add(densematrix1, false, densematrix1, true).sum() - sum(bdensematrix1.t + bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.add(densematrix2, densematrix2).sum() - sum(bdensematrix2.t + bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.add(densematrix2, true, densematrix2, true).sum() - sum(bdensematrix2 + bdensematrix2)) < 1)
    assert(abs(Ufuncs.add(densematrix2, true, densematrix2, false).sum() - sum(bdensematrix2 + bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.add(densematrix2, false, densematrix2, true).sum() - sum(bdensematrix2.t + bdensematrix2)) < 1)
  }


  test("binary sub test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sub(densematrix1, densematrix1)
      Ufuncs.sub(densematrix1, true, densematrix1, true)
      Ufuncs.sub(densematrix1, true, densematrix1, false)
      Ufuncs.sub(densematrix1, false, densematrix1, true)
      Ufuncs.sub(densematrix2, densematrix2)
      Ufuncs.sub(densematrix2, true, densematrix2, true)
      Ufuncs.sub(densematrix2, true, densematrix2, false)
      Ufuncs.sub(densematrix2, false, densematrix2, true)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t - bdensematrix1.t
      bdensematrix1 - bdensematrix1
      bdensematrix1.t - bdensematrix1
      bdensematrix1 - bdensematrix1.t
      bdensematrix2.t - bdensematrix2.t
      bdensematrix2 - bdensematrix2
      bdensematrix2 - bdensematrix2.t
      bdensematrix2.t - bdensematrix2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.sub(densematrix1, densematrix1).sum() - sum(bdensematrix1.t - bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.sub(densematrix1, true, densematrix1, true).sum() - sum(bdensematrix1 - bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.sub(densematrix1, true, densematrix1, false).sum() - sum(bdensematrix1 - bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.sub(densematrix1, false, densematrix1, true).sum() - sum(bdensematrix1.t - bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.sub(densematrix2, densematrix2).sum() - sum(bdensematrix2.t - bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.sub(densematrix2, true, densematrix2, true).sum() - sum(bdensematrix2 - bdensematrix2)) < 1)
    assert(abs(Ufuncs.sub(densematrix2, true, densematrix2, false).sum() - sum(bdensematrix2 - bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.sub(densematrix2, false, densematrix2, true).sum() - sum(bdensematrix2.t - bdensematrix2)) < 1)
  }


  test("binary mul test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.mul(densematrix1, densematrix1)
      Ufuncs.mul(densematrix1, true, densematrix1, true)
      Ufuncs.mul(densematrix1, true, densematrix1, false)
      Ufuncs.mul(densematrix1, false, densematrix1, true)
      Ufuncs.mul(densematrix2, densematrix2)
      Ufuncs.mul(densematrix2, true, densematrix2, true)
      Ufuncs.mul(densematrix2, true, densematrix2, false)
      Ufuncs.mul(densematrix2, false, densematrix2, true)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t :* bdensematrix1.t
      bdensematrix1 :* bdensematrix1
      bdensematrix1.t :* bdensematrix1
      bdensematrix1 :* bdensematrix1.t
      bdensematrix2.t :* bdensematrix2.t
      bdensematrix2 :* bdensematrix2
      bdensematrix2 :* bdensematrix2.t
      bdensematrix2.t :* bdensematrix2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")


    assert(abs(Ufuncs.mul(densematrix1, densematrix1).sum() - sum(bdensematrix1.t :* bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.mul(densematrix1, true, densematrix1, true).sum() - sum(bdensematrix1 :* bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.mul(densematrix1, true, densematrix1, false).sum() - sum(bdensematrix1 :* bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.mul(densematrix1, false, densematrix1, true).sum() - sum(bdensematrix1.t :* bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.mul(densematrix2, densematrix2).sum() - sum(bdensematrix2.t :* bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.mul(densematrix2, true, densematrix2, true).sum() - sum(bdensematrix2 :* bdensematrix2)) < 1)
    assert(abs(Ufuncs.mul(densematrix2, true, densematrix2, false).sum() - sum(bdensematrix2 :* bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.mul(densematrix2, false, densematrix2, true).sum() - sum(bdensematrix2.t :* bdensematrix2)) < 1)
  }

  test("binary div test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.div(densematrix1, densematrix1)
      Ufuncs.div(densematrix1, true, densematrix1, true)
      Ufuncs.div(densematrix1, true, densematrix1, false)
      Ufuncs.div(densematrix1, false, densematrix1, true)
      Ufuncs.div(densematrix2, densematrix2)
      Ufuncs.div(densematrix2, true, densematrix2, true)
      Ufuncs.div(densematrix2, true, densematrix2, false)
      Ufuncs.div(densematrix2, false, densematrix2, true)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1 :/ bdensematrix1
      bdensematrix1.t :/ bdensematrix1.t
      bdensematrix1.t :/ bdensematrix1
      bdensematrix1 :/ bdensematrix1.t
      bdensematrix2 :/ bdensematrix2
      bdensematrix2.t :/ bdensematrix2.t
      bdensematrix2.t :/ bdensematrix2
      bdensematrix2 :/ bdensematrix2.t
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.div(densematrix1, densematrix1).sum() - sum(bdensematrix1.t :/ bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.div(densematrix1, true, densematrix1, true).sum() - sum(bdensematrix1 :/ bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.div(densematrix1, true, densematrix1, false).sum() - sum(bdensematrix1 :/ bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.div(densematrix1, false, densematrix1, true).sum() - sum(bdensematrix1.t :/ bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.div(densematrix2, densematrix2).sum() - sum(bdensematrix2.t :/ bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.div(densematrix2, true, densematrix2, true).sum() - sum(bdensematrix2 :/ bdensematrix2)) < 1)
    assert(abs(Ufuncs.div(densematrix2, true, densematrix2, false).sum() - sum(bdensematrix2 :/ bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.div(densematrix2, false, densematrix2, true).sum() - sum(bdensematrix2.t :/ bdensematrix2)) < 1)
  }


  test("binary axpy test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.axpy(densematrix1, densematrix1, 2.0)
      Ufuncs.axpy(densematrix1, true, densematrix1, true, 2.0)
      Ufuncs.axpy(densematrix1, true, densematrix1, false, 2.0)
      Ufuncs.axpy(densematrix1, false, densematrix1, true, 2.0)
      Ufuncs.axpy(densematrix2, densematrix2, 2.0)
      Ufuncs.axpy(densematrix2, true, densematrix2, true, 2.0)
      Ufuncs.axpy(densematrix2, true, densematrix2, false, 2.0)
      Ufuncs.axpy(densematrix2, false, densematrix2, true, 2.0)


    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      axpy(2.0, bdensematrix1.t, bdensematrix1.t)
      axpy(2.0, bdensematrix1, bdensematrix1)
      axpy(2.0, bdensematrix1, bdensematrix1.t)
      axpy(2.0, bdensematrix1.t, bdensematrix1)
      axpy(2.0f, bdensematrix2.t, bdensematrix2.t)
      axpy(2.0f, bdensematrix2, bdensematrix2)
      axpy(2.0f, bdensematrix2, bdensematrix2.t)
      axpy(2.0f, bdensematrix2.t, bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense axpy:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
  }

  test("binary dot test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.dot(densematrix1, densematrix1)
      Ufuncs.dot(densematrix1, true, densematrix1, true)
      Ufuncs.dot(densematrix1, true, densematrix1, false)
      Ufuncs.dot(densematrix1, false, densematrix1, true)
      Ufuncs.dot(densematrix2, densematrix2)
      Ufuncs.dot(densematrix2, true, densematrix2, true)
      Ufuncs.dot(densematrix2, true, densematrix2, false)
      Ufuncs.dot(densematrix2, false, densematrix2, true)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t * bdensematrix1.t
      bdensematrix1 * bdensematrix1
      bdensematrix1 * bdensematrix1.t
      bdensematrix1.t * bdensematrix1
      bdensematrix2.t * bdensematrix2.t
      bdensematrix2 * bdensematrix2
      bdensematrix2 * bdensematrix2.t
      bdensematrix2.t * bdensematrix2

    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.dot(densematrix1, densematrix1).sum() - sum(bdensematrix1.t * bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix1, true, densematrix1, true).sum() - sum(bdensematrix1 * bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix1, true, densematrix1, false).sum() - sum(bdensematrix1 * bdensematrix1.t)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix1, false, densematrix1, true).sum() - sum(bdensematrix1.t * bdensematrix1)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix2, densematrix2).sum() - sum(bdensematrix2.t * bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.dot(densematrix2, true, densematrix2, true).sum() - sum(bdensematrix2 * bdensematrix2)) < 1)
    assert(abs(Ufuncs.dot(densematrix2, true, densematrix2, false).sum() - sum(bdensematrix2 * bdensematrix2.t)) < 1)
    assert(abs(Ufuncs.dot(densematrix2, false, densematrix2, true).sum() - sum(bdensematrix2.t * bdensematrix2)) < 1)
  }

  test("densematrix dot densevector test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      densematrix1.dot(ilist.get(0))
      densematrix2.dot(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t * dense1
      bdensematrix2.t * dense2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel densematrix dot densevector :$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.dot(densematrix1, ilist.get(0)).sum() - sum(bdensematrix1.t * dense1)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix2, ilist.get(3)).sum() - sum(bdensematrix2.t * dense2)) < 1)
  }

  test("densematrix dot vector test") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      densematrix1.dot(ilist.get(1))
      densematrix1.dot(ilist.get(2))
      densematrix2.dot(ilist.get(4))
      densematrix2.dot(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      bdensematrix1.t * sparse1
      bdensematrix1.t * sorted1
      bdensematrix2.t * sparse2
      bdensematrix2.t * sorted2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel densematrix dot vector :$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(Ufuncs.dot(densematrix1, ilist.get(1)).sum() - sum(bdensematrix1.t * sparse1)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix2, ilist.get(4)).sum() - sum(bdensematrix2.t * sparse2)) < 1)
    assert(abs(Ufuncs.dot(densematrix1, ilist.get(2)).sum() - sum(bdensematrix1.t * sorted1)) < 1.0E-8)
    assert(abs(Ufuncs.dot(densematrix2, ilist.get(5)).sum() - sum(bdensematrix2.t * sorted2)) < 1)
  }

  test("exp") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(densematrix1)
      Ufuncs.exp(densematrix2)

    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      exp(bdensematrix1)
      exp(bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense exp:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(Ufuncs.exp(densematrix1).sum() == sum(exp(bdensematrix1)))
    assert(abs(Ufuncs.exp(densematrix2).sum() - sum(exp(bdensematrix2))) < 1)
  }

  test("log") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(densematrix1)
      Ufuncs.log(densematrix2)

    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log(bdensematrix1)
      log(bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense log:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(Ufuncs.log(densematrix1).sum() == sum(log(bdensematrix1)))
    assert(abs(Ufuncs.log(densematrix2).sum() - sum(log(bdensematrix2))) < 1)
  }

  test("log1p") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(densematrix1)
      Ufuncs.log1p(densematrix2)

    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log1p(bdensematrix1)
      log1p(bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense log1p:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(Ufuncs.log1p(densematrix1).sum() == sum(log1p(bdensematrix1)))
    assert(abs(Ufuncs.log1p(densematrix2).sum() - sum(log1p(bdensematrix2))) < 1)
  }

  test("pow") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(densematrix1, 2.0)
      Ufuncs.pow(densematrix2, 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(bdensematrix1, 2.0)
      pow(bdensematrix2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(Ufuncs.pow(densematrix1, 2.0).sum() == sum(pow(bdensematrix1, 2.0)))
    assert(abs(Ufuncs.pow(densematrix2, 2.0).sum() - sum(pow(bdensematrix2, 2.0f))) < 1)
  }

  test("sigmoid") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(densematrix1)
      TransFuncs.sigmoid(densematrix2)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sigmoid(bdensematrix1)
      sigmoid(bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sigmoid:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(TransFuncs.sigmoid(densematrix1).sum() == sum(sigmoid(bdensematrix1)))
    assert(abs(TransFuncs.sigmoid(densematrix2).sum() - sum(sigmoid(bdensematrix2))) < 1)
  }

  test("sqrt") {
    init()
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(densematrix1)
      Ufuncs.sqrt(densematrix2)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(bdensematrix1)
      sqrt(bdensematrix2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(Ufuncs.sqrt(densematrix1).sum() == sum(sqrt(bdensematrix1)))
    assert(abs(Ufuncs.sqrt(densematrix2).sum() - sum(sqrt(bdensematrix2))) < 1)
  }
}
