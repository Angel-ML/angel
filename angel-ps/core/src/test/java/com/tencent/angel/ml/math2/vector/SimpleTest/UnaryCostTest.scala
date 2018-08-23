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
import breeze.linalg.{DenseVector, HashVector, SparseVector}
import breeze.numerics._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}


object UnaryCostTest {
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

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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
      longValues(i) = rand.nextInt(100);
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(100)
    }


    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(100)
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(100)
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

class UnaryCostTest {
  val capacity: Int = UnaryCostTest.capacity
  val dim: Int = UnaryCostTest.dim

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val ilist = UnaryCostTest.ilist

  var sparse1 = UnaryCostTest.sparse1
  var dense1 = UnaryCostTest.dense1
  var sorted1 = UnaryCostTest.sorted1

  var dense2 = UnaryCostTest.dense2
  var sparse2 = UnaryCostTest.sparse2
  var sorted2 = UnaryCostTest.sorted2

  var dense3 = UnaryCostTest.dense3
  var sparse3 = UnaryCostTest.sparse3
  var sorted3 = UnaryCostTest.sorted3

  var dense4 = UnaryCostTest.dense4
  var sparse4 = UnaryCostTest.sparse4
  var sorted4 = UnaryCostTest.sorted4

  @Test
  def expTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(ilist.get(0))
      Ufuncs.exp(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      exp(dense1)
      exp(dense2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense exp:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(ilist.get(1))
      Ufuncs.exp(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      exp(sparse1)
      exp(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse exp:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(ilist.get(2))
      Ufuncs.exp(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      exp(sorted1)
      exp(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted exp:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
  }

  @Test
  def logTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(ilist.get(0))
      Ufuncs.log(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log(dense1)
      log(dense2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense log:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(ilist.get(1))
      Ufuncs.log(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log(sparse1)
      log(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse log:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(ilist.get(2))
      Ufuncs.log(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log(sorted1)
      log(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted log:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
  }

  @Test
  def log1pTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(ilist.get(0))
      Ufuncs.log1p(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log1p(dense1)
      log1p(dense2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense log1p:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(ilist.get(1))
      Ufuncs.log1p(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log1p(sparse1)
      log1p(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse log1p:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(ilist.get(2))
      Ufuncs.log1p(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      log1p(sorted1)
      log1p(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted log1p:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def sigmoidTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(ilist.get(0))
      TransFuncs.sigmoid(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sigmoid(dense1)
      sigmoid(dense2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sigmoid:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(ilist.get(1))
      TransFuncs.sigmoid(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sigmoid(sparse1)
      sigmoid(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sigmoid:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(ilist.get(2))
      TransFuncs.sigmoid(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sigmoid(sorted1)
      sigmoid(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sigmoid:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def softthresholdTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.softthreshold(ilist.get(0), 1.5)
      Ufuncs.softthreshold(ilist.get(3), 1.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    val d1 = abs(dense1) - 1.5
    (0 until dim).foreach { i =>
      d1(i) = if (d1(i) < 0) 0.0 else d1(i)
    }
    val d2 = abs(dense2) - 1.5f
    (0 until dim).foreach { i =>
      d2(i) = if (d2(i) < 0f) 0.0f else d2(i)
    }
    (0 to times).foreach { _ =>
      sin(dense1) :* d1
      sin(dense2) :* d2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense softthreshold:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.softthreshold(ilist.get(1), 1.5)
      Ufuncs.softthreshold(ilist.get(4), 1.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    val sp1 = abs(sparse1) - 1.5
    (0 until dim).foreach { i =>
      sp1(i) = if (sp1(i) < 0) 0.0 else sp1(i)
    }
    val sp2 = abs(sparse2) - 1.5f
    (0 until dim).foreach { i =>
      sp2(i) = if (sp2(i) < 0f) 0.0f else sp2(i)
    }
    (0 to times).foreach { _ =>
      sin(sparse1) :* sp1
      sin(sparse2) :* sp2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse softthreshold:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.softthreshold(ilist.get(2), 1.5)
      Ufuncs.softthreshold(ilist.get(5), 1.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    val st1 = abs(sorted1) - 1.5
    (0 until dim).foreach { i =>
      st1(i) = if (st1(i) < 0) 0.0 else st1(i)
    }
    val st2 = abs(sorted2) - 1.5f
    (0 until dim).foreach { i =>
      st2(i) = if (st2(i) < 0f) 0.0f else st2(i)
    }
    (0 to times).foreach { _ =>
      sin(sorted1) :* st1
      sin(sorted2) :* st2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted softthreshold:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def saddTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sadd(ilist.get(0), 0.5)
      Ufuncs.sadd(ilist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 + 0.5
      dense2 + 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sadd:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sadd(ilist.get(1), 0.5)
      Ufuncs.sadd(ilist.get(4), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 + 0.5
      sparse2 + 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sadd:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sadd(ilist.get(2), 0.5)
      Ufuncs.sadd(ilist.get(5), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 + 0.5
      sorted2 + 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sadd:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def ssubTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.ssub(ilist.get(0), 0.5)
      Ufuncs.ssub(ilist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 - 0.5
      dense2 - 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense ssub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.ssub(ilist.get(1), 0.5)
      Ufuncs.ssub(ilist.get(4), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 - 0.5
      sparse2 - 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse ssub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.ssub(ilist.get(2), 0.5)
      Ufuncs.ssub(ilist.get(5), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 - 0.5
      sorted2 - 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted ssub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def powTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(ilist.get(0), 2.0)
      Ufuncs.pow(ilist.get(3), 2.0f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(dense1, 2.0)
      pow(dense2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(ilist.get(1), 2.0)
      Ufuncs.pow(ilist.get(4), 2.0f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(sparse1, 2.0)
      pow(sparse2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(ilist.get(2), 2.0)
      Ufuncs.pow(ilist.get(5), 2.0f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(sorted1, 2.0)
      pow(sorted2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def sqrtTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(0))
      Ufuncs.sqrt(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(dense1)
      sqrt(dense2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(1))
      Ufuncs.sqrt(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sparse1)
      sqrt(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(2))
      Ufuncs.sqrt(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sorted1)
      sqrt(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def smulTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(ilist.get(0), 0.5)
      Ufuncs.smul(ilist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :* 0.5
      dense2 :* 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(ilist.get(1), 0.5)
      Ufuncs.smul(ilist.get(4), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :* 0.5
      sparse2 :* 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(ilist.get(2), 0.5)
      Ufuncs.smul(ilist.get(5), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :* 0.5
      sorted2 :* 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

  }

  @Test
  def sdivTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(ilist.get(0), 0.5)
      Ufuncs.sdiv(ilist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :/ 0.5
      dense2 :/ 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(ilist.get(1), 0.5)
      Ufuncs.sdiv(ilist.get(4), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :/ 0.5
      sparse2 :/ 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(ilist.get(2), 0.5)
      Ufuncs.sdiv(ilist.get(5), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :/ 0.5
      sorted2 :/ 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")


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
