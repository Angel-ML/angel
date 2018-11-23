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


package com.tencent.angel.ml.math2.vector.SimpleTest

import java.util

import breeze.collection.mutable.{OpenAddressHashArray, SparseArray}
import breeze.linalg.{DenseVector, HashVector, SparseVector, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object UnaryIntKeyTest {
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

  val ilist = new util.ArrayList[Vector]()


  var dense1 = DenseVector[Double](densedoubleValues)
  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

  var dense2 = DenseVector[Float](densefloatValues)
  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var dense3 = DenseVector[Long](denselongValues)
  var sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var dense4 = DenseVector[Int](denseintValues)
  var sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

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


    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(100) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(100) + 1
    }

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    dense1 = DenseVector[Double](densedoubleValues)
    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

    ilist.add(VFactory.denseFloatVector(densefloatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))

    dense2 = DenseVector[Float](densefloatValues)
    sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse2(i) = v }
    sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

    ilist.add(VFactory.denseLongVector(denselongValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))

    dense3 = DenseVector[Long](denselongValues)
    sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse3(i) = v }
    sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

    ilist.add(VFactory.denseIntVector(denseintValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))

    dense4 = DenseVector[Int](denseintValues)
    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))
  }

}

class UnaryIntKeyTest {
  val capacity: Int = UnaryIntKeyTest.capacity
  val dim: Int = UnaryIntKeyTest.dim

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val ilist = UnaryIntKeyTest.ilist

  var sparse1 = UnaryIntKeyTest.sparse1
  var dense1 = UnaryIntKeyTest.dense1
  var sorted1 = UnaryIntKeyTest.sorted1

  var dense2 = UnaryIntKeyTest.dense2
  var sparse2 = UnaryIntKeyTest.sparse2
  var sorted2 = UnaryIntKeyTest.sorted2

  var dense3 = UnaryIntKeyTest.dense3
  var sparse3 = UnaryIntKeyTest.sparse3
  var sorted3 = UnaryIntKeyTest.sorted3

  var dense4 = UnaryIntKeyTest.dense4
  var sparse4 = UnaryIntKeyTest.sparse4
  var sorted4 = UnaryIntKeyTest.sorted4

  @Test
  def expTest() {
    assert(abs(Ufuncs.exp(ilist.get(0)).sum() - sum(exp(dense1)))< 1.0)
    assert(abs(Ufuncs.exp(ilist.get(1)).sum() - sum(exp(sparse1))) < 1.0)
    assert(abs(Ufuncs.exp(ilist.get(2)).sum() - sum(exp(sorted1))) < 1.0)
  }

  @Test
  def log1pTest() {
    assert(abs(Ufuncs.log1p(ilist.get(0)).sum() - sum(log1p(dense1))) < 1.0)
    assert(abs(Ufuncs.log1p(ilist.get(1)).sum() - sum(log1p(sparse1))) < 1.0)
    assert(abs(Ufuncs.log1p(ilist.get(2)).sum() - sum(log1p(sorted1))) < 1.0)
  }

  @Test
  def sigmoidTest() {
    assert(abs(TransFuncs.sigmoid(ilist.get(0)).sum() - sum(sigmoid(dense1))) < 1.0)
    assert(abs(TransFuncs.sigmoid(ilist.get(1)).sum() - sum(sigmoid(sparse1))) < 1.0)
    assert(abs(TransFuncs.sigmoid(ilist.get(2)).sum() - sum(sigmoid(sorted1))) < 1.0)
  }

  @Test
  def softthresholdTest() {
    val d1 = abs(dense1) - 1.5
    (0 until dim).foreach { i =>
      d1(i) = if (d1(i) < 0) 0.0 else d1(i)
    }
    val d2 = abs(dense2) - 1.5f
    (0 until dim).foreach { i =>
      d2(i) = if (d2(i) < 0f) 0.0f else d2(i)
    }

    assert(abs(Ufuncs.softthreshold(ilist.get(0), 1.5).sum() - sum(sin(dense1) :* d1)) < 1.0)
    assert(abs(Ufuncs.softthreshold(ilist.get(1), 1.5).sum() - sum(sin(sparse1) :* d1)) < 1.0)
    assert(abs(Ufuncs.softthreshold(ilist.get(2), 1.5).sum() - sum(sin(sorted1) :* d1)) < 1.0)
  }

  @Test
  def saddTest() {
    assert(abs(Ufuncs.sadd(ilist.get(0), 0.5).sum() - sum(dense1 + 0.5))< 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(1), 0.5).sum() - sum(sparse1 + 0.5)) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(2), 0.5).sum() - sum(sorted1 + 0.5)) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(6), 2).sum() - sum((dense3 + 2L)))< 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(7), 2).sum() - sum((sparse3 + 2L))) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(9), 2).sum() - sum((dense4 + 2))) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(10), 2).sum() - sum((sparse4 + 2))) < 1.0)
  }

  @Test
  def ssubTest() {
    assert(abs(Ufuncs.ssub(ilist.get(0), 0.5).sum() - sum(dense1 - 0.5)) < 1.0)
    assert(abs(Ufuncs.ssub(ilist.get(1), 0.5).sum() - sum(sparse1 - 0.5)) < 1.0)
    assert(abs(Ufuncs.ssub(ilist.get(2), 0.5).sum() - sum(sorted1 - 0.5)) < 1.0)

  }

  @Test
  def powTest() {
    assert(abs(Ufuncs.pow(ilist.get(0), 2.0).sum() - sum(pow(dense1, 2.0)))< 1.0)
    assert(abs(Ufuncs.pow(ilist.get(1), 2.0).sum() - sum(pow(sparse1, 2.0))) < 1.0)
    assert(abs(Ufuncs.pow(ilist.get(2), 2.0).sum() - sum(pow(sorted1, 2.0)))< 1.0)
  }

  @Test
  def sqrtTest() {
    assert(abs(Ufuncs.sqrt(ilist.get(0)).sum() - sum(sqrt(dense1)))< 1.0)
    assert(abs(Ufuncs.sqrt(ilist.get(1)).sum() - sum(sqrt(sparse1))) < 1.0)
    assert(abs(Ufuncs.sqrt(ilist.get(2)).sum() - sum(sqrt(sorted1)))< 1.0)
  }

  @Test
  def smulTest() {
    assert(abs(Ufuncs.smul(ilist.get(0), 0.5).sum() - sum(dense1 :* 0.5))< 1.0)
    assert(abs(Ufuncs.smul(ilist.get(1), 0.5).sum() - sum(sparse1 :* 0.5)) < 1.0)
    assert(abs(Ufuncs.smul(ilist.get(2), 0.5).sum() - sum(sorted1 :* 0.5))< 1.0)
    assert(abs(Ufuncs.smul(ilist.get(6), 5).sum() - sum(dense3 :* 5L))< 1.0)
    assert(abs(Ufuncs.smul(ilist.get(7), 5).sum() - sum(sparse3 :* 5L)) < 1.0)
    assert(abs(Ufuncs.smul(ilist.get(8), 5).sum() - sum(sorted3 :* 5L))< 1.0)
  }

  @Test
  def sdivTest() {
    assert(abs(Ufuncs.sdiv(ilist.get(0), 0.5).sum() - sum(dense1 :/ 0.5)) < 1.0)
    assert(abs(Ufuncs.sdiv(ilist.get(1), 0.5).sum() - sum(sparse1 :/ 0.5)) < 1.0)
    assert(abs(Ufuncs.sdiv(ilist.get(2), 0.5).sum() - sum(sorted1 :/ 0.5)) < 1.0)

  }

  def getFlag(v: Vector): String = {
    v match {
      case _: IntDummyVector => "dummy"
      case _: LongDummyVector => "dummy"
      case x if x.isDense => "dense"
      case x if x.isSparse => "sparse"
      case x if x.isSorted => "sorted"
      case _ => "dummy"
    }
  }
}
