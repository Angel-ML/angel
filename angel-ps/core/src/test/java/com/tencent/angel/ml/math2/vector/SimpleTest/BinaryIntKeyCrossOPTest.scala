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
import breeze.linalg.{DenseVector, HashVector, SparseVector, axpy, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object BinaryIntKeyCrossOPTest {
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

  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var dense1 = DenseVector[Double](densedoubleValues)
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


  @BeforeClass
  def init(): Unit = {
    println("init---------------------")
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
      denseintValues(i) = rand.nextInt(50) + 1
    }

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    dense1 = DenseVector[Double](densedoubleValues)
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

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

class BinaryIntKeyCrossOPTest {
  val capacity: Int = BinaryIntKeyCrossOPTest.capacity
  val dim: Int = BinaryIntKeyCrossOPTest.dim

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val ilist = BinaryIntKeyCrossOPTest.ilist

  var sparse1 = BinaryIntKeyCrossOPTest.sparse1
  var dense1 = BinaryIntKeyCrossOPTest.dense1
  var sorted1 = BinaryIntKeyCrossOPTest.sorted1

  var dense2 = BinaryIntKeyCrossOPTest.dense2
  var sparse2 = BinaryIntKeyCrossOPTest.sparse2
  var sorted2 = BinaryIntKeyCrossOPTest.sorted2

  var dense3 = BinaryIntKeyCrossOPTest.dense3
  var sparse3 = BinaryIntKeyCrossOPTest.sparse3
  var sorted3 = BinaryIntKeyCrossOPTest.sorted3

  var dense4 = BinaryIntKeyCrossOPTest.dense4
  var sparse4 = BinaryIntKeyCrossOPTest.sparse4
  var sorted4 = BinaryIntKeyCrossOPTest.sorted4

  @Test
  def Addtest() {
    Thread.sleep(600L)
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).add(ilist.get(0))
      ilist.get(3).add(ilist.get(3))
      ilist.get(6).add(ilist.get(6))
      ilist.get(9).add(ilist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 + dense1
      dense2 + dense2
      dense3 + dense3
      dense4 + dense4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(ilist.get(0).add(ilist.get(0)).sum() == sum(dense1 + dense1))
    assert(abs(ilist.get(3).add(ilist.get(3)).sum() - sum(dense2 + dense2)) < 1.0)
    assert(ilist.get(6).add(ilist.get(6)).sum() == sum(dense3 + dense3))
    assert(ilist.get(9).add(ilist.get(9)).sum() == sum(dense4 + dense4))

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).add(ilist.get(1))
      ilist.get(4).add(ilist.get(4))
      ilist.get(7).add(ilist.get(7))
      ilist.get(10).add(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 + sparse1
      sparse2 + sparse2
      sparse3 + sparse3
      sparse4 + sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(1).add(ilist.get(1)).sum() - sum(sparse1 + sparse1)) < 1.0E-8)
    assert(abs(ilist.get(4).add(ilist.get(4)).sum() - sum(sparse2 + sparse2)) < 1.0E-1)
    assert(abs(ilist.get(7).add(ilist.get(7)).sum() - sum(sparse3 + sparse3)) < 1.0E-8)
    assert(abs(ilist.get(10).add(ilist.get(10)).sum() - sum(sparse4 + sparse4)) < 1.0E-8)

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).add(ilist.get(2))
      ilist.get(5).add(ilist.get(5))
      ilist.get(8).add(ilist.get(8))
      ilist.get(11).add(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 + sorted1
      sorted2 + sorted2
      sorted3 + sorted3
      sorted4 + sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(2).add(ilist.get(2)).sum() - sum(sorted1 + sorted1)) < 1.0E-8)
    assert(abs(ilist.get(5).add(ilist.get(5)).sum() - sum(sorted2 + sorted2)) < 1.0E-1)
    assert(abs(ilist.get(8).add(ilist.get(8)).sum() - sum(sorted3 + sorted3)) < 1.0E-8)
    assert(abs(ilist.get(11).add(ilist.get(11)).sum() - sum(sorted4 + sorted4)) < 1.0E-8)

    println("------------------------dense sparse-------------------------")

    //dense sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).add(ilist.get(1))
      ilist.get(3).add(ilist.get(4))
      ilist.get(6).add(ilist.get(7))
      ilist.get(9).add(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 + sparse1
      dense2 + sparse2
      dense3 + sparse3
      dense4 + sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sparse add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).add(ilist.get(1)).sum() - sum(dense1 + sparse1)) < 1.0E-8)
    assert(abs(ilist.get(3).add(ilist.get(4)).sum() - sum(dense2 + sparse2)) < 1.0)
    assert(abs(ilist.get(6).add(ilist.get(7)).sum() - sum(dense3 + sparse3)) < 1.0E-8)
    assert(abs(ilist.get(9).add(ilist.get(10)).sum() - sum(dense4 + sparse4)) < 1.0E-8)

    println("-------------------------------dense sorted-----------------------")

    //dense sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).add(ilist.get(2))
      ilist.get(3).add(ilist.get(5))
      ilist.get(6).add(ilist.get(8))
      ilist.get(9).add(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 + sorted1
      dense2 + sorted2
      dense3 + sorted3
      dense4 + sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sorted add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).add(ilist.get(2)).sum() - sum(dense1 + sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).add(ilist.get(5)).sum() - sum(dense2 + sorted2)) < 1.0)
    assert(abs(ilist.get(6).add(ilist.get(8)).sum() - sum(dense3 + sorted3)) < 1.0E-8)
    assert(abs(ilist.get(9).add(ilist.get(11)).sum() - sum(dense4 + sorted4)) < 1.0E-8)

    println("------------------------sparse sorted------------------")

    //sparse sorted  cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).add(ilist.get(2))
      ilist.get(4).add(ilist.get(5))
      ilist.get(7).add(ilist.get(8))
      ilist.get(10).add(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 + sorted1
      sparse2 + sorted2
      sparse3 + sorted3
      sparse4 + sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sorted add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    Thread.sleep(6000L)
    assert(abs(ilist.get(1).add(ilist.get(2)).sum() - sum(sparse1 + sorted1)) < 1.0E-8)
    assert(abs(ilist.get(4).add(ilist.get(5)).sum() - sum(sparse2 + sorted2)) < 1.0E-2)
    assert(abs(ilist.get(7).add(ilist.get(8)).sum() - sum(sparse3 + sorted3)) < 1.0E-8)
    assert(abs(ilist.get(10).add(ilist.get(11)).sum() - sum(sparse4 + sorted4)) < 1.0E-8)
  }

  @Test
  def Subtest() {
    Thread.sleep(600L)
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).sub(ilist.get(0))
      ilist.get(3).sub(ilist.get(3))
      ilist.get(6).sub(ilist.get(6))
      ilist.get(9).sub(ilist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 - dense1
      dense2 - dense2
      dense3 - dense3
      dense4 - dense4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(ilist.get(0).sub(ilist.get(0)).sum() == sum(dense1 - dense1))
    assert(abs(ilist.get(3).sub(ilist.get(3)).sum() - sum(dense2 - dense2)) < 1.0E-8)
    assert(ilist.get(6).sub(ilist.get(6)).sum() == sum(dense3 - dense3))
    assert(ilist.get(9).sub(ilist.get(9)).sum() == sum(dense4 - dense4))


    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).sub(ilist.get(1))
      ilist.get(4).sub(ilist.get(4))
      ilist.get(7).sub(ilist.get(7))
      ilist.get(10).sub(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 - sparse1
      sparse2 - sparse2
      sparse3 - sparse3
      sparse4 - sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(1).sub(ilist.get(1)).sum() - sum(sparse1 - sparse1)) < 1.0E-8)
    assert(abs(ilist.get(4).sub(ilist.get(4)).sum() - sum(sparse2 - sparse2)) < 1.0E-8)
    assert(abs(ilist.get(7).sub(ilist.get(7)).sum() - sum(sparse3 - sparse3)) < 1.0E-8)
    assert(abs(ilist.get(10).sub(ilist.get(10)).sum() - sum(sparse4 - sparse4)) < 1.0E-8)

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).sub(ilist.get(2))
      ilist.get(5).sub(ilist.get(5))
      ilist.get(8).sub(ilist.get(8))
      ilist.get(11).sub(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 - sorted1
      sorted2 - sorted2
      sorted3 - sorted3
      sorted4 - sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(2).sub(ilist.get(2)).sum() - sum(sorted1 - sorted1)) < 1.0E-8)
    assert(abs(ilist.get(5).sub(ilist.get(5)).sum() - sum(sorted2 - sorted2)) < 1.0E-8)
    assert(abs(ilist.get(8).sub(ilist.get(8)).sum() - sum(sorted3 - sorted3)) < 1.0E-8)
    assert(abs(ilist.get(11).sub(ilist.get(11)).sum() - sum(sorted4 - sorted4)) < 1.0E-8)

    println("------------------------dense sparse-------------------------")

    //dense sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).sub(ilist.get(1))
      ilist.get(3).sub(ilist.get(4))
      ilist.get(6).sub(ilist.get(7))
      ilist.get(9).sub(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 - sparse1
      dense2 - sparse2
      dense3 - sparse3
      dense4 - sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sparse sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).sub(ilist.get(1)).sum() - sum(dense1 - sparse1)) < 1.0E-8)
    assert(abs(ilist.get(3).sub(ilist.get(4)).sum() - sum(dense2 - sparse2)) < 1.0)
    assert(abs(ilist.get(6).sub(ilist.get(7)).sum() - sum(dense3 - sparse3)) < 1.0E-8)
    assert(abs(ilist.get(9).sub(ilist.get(10)).sum() - sum(dense4 - sparse4)) < 1.0E-8)

    println("-------------------------------dense sorted-----------------------")

    //dense sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).sub(ilist.get(2))
      ilist.get(3).sub(ilist.get(5))
      ilist.get(6).sub(ilist.get(8))
      ilist.get(9).sub(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 - sorted1
      dense2 - sorted2
      dense3 - sorted3
      dense4 - sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sorted sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).sub(ilist.get(2)).sum() - sum(dense1 - sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).sub(ilist.get(5)).sum() - sum(dense2 - sorted2)) < 1.0)
    assert(abs(ilist.get(6).sub(ilist.get(8)).sum() - sum(dense3 - sorted3)) < 1.0E-8)
    assert(abs(ilist.get(9).sub(ilist.get(11)).sum() - sum(dense4 - sorted4)) < 1.0E-8)

    println("------------------------sparse sorted------------------")

    //sparse sorted  cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).sub(ilist.get(2))
      ilist.get(4).sub(ilist.get(5))
      ilist.get(7).sub(ilist.get(8))
      ilist.get(10).sub(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 - sorted1
      sparse2 - sorted2
      sparse3 - sorted3
      sparse4 - sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sorted sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    Thread.sleep(600L)

    assert(abs(ilist.get(1).sub(ilist.get(2)).sum() - sum(sparse1 - sorted1)) < 1.0E-8)
    assert(abs(ilist.get(4).sub(ilist.get(5)).sum() - sum(sparse2 - sorted2)) < 1.0E-3)
    assert(abs(ilist.get(7).sub(ilist.get(8)).sum() - sum(sparse3 - sorted3)) < 1.0E-8)
    assert(abs(ilist.get(10).sub(ilist.get(11)).sum() - sum(sparse4 - sorted4)) < 1.0E-8)
  }

  @Test
  def Multest() {
    Thread.sleep(600L)
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).mul(ilist.get(0))
      ilist.get(3).mul(ilist.get(3))
      ilist.get(6).mul(ilist.get(6))
      ilist.get(9).mul(ilist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :* dense1
      dense2 :* dense2
      dense3 :* dense3
      dense4 :* dense4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(ilist.get(0).mul(ilist.get(0)).sum() == sum(dense1 :* dense1))
    assert(abs(ilist.get(3).mul(ilist.get(3)).sum() - sum(dense2 :* dense2)) < 1.0)
    assert(ilist.get(6).mul(ilist.get(6)).sum() == sum(dense3 :* dense3))
    assert(ilist.get(9).mul(ilist.get(9)).sum() == sum(dense4 :* dense4))

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).mul(ilist.get(1))
      ilist.get(4).mul(ilist.get(4))
      ilist.get(7).mul(ilist.get(7))
      ilist.get(10).mul(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :* sparse1
      sparse2 :* sparse2
      sparse3 :* sparse3
      sparse4 :* sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(1).mul(ilist.get(1)).sum() - sum(sparse1 :* sparse1)) < 1.0E-8)
    assert(abs(ilist.get(4).mul(ilist.get(4)).sum() - sum(sparse2 :* sparse2)) < 1.0E-2)
    assert(abs(ilist.get(7).mul(ilist.get(7)).sum() - sum(sparse3 :* sparse3)) < 1.0E-8)
    assert(abs(ilist.get(10).mul(ilist.get(10)).sum() - sum(sparse4 :* sparse4)) < 1.0E-8)

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).mul(ilist.get(2))
      ilist.get(5).mul(ilist.get(5))
      ilist.get(8).mul(ilist.get(8))
      ilist.get(11).mul(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :* sorted1
      sorted2 :* sorted2
      sorted3 :* sorted3
      sorted4 :* sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(2).mul(ilist.get(2)).sum() - sum(sorted1 :* sorted1)) < 1.0E-8)
    assert(abs(ilist.get(5).mul(ilist.get(5)).sum() - sum(sorted2 :* sorted2)) < 1.0E-2)
    assert(abs(ilist.get(8).mul(ilist.get(8)).sum() - sum(sorted3 :* sorted3)) < 1.0E-8)
    assert(abs(ilist.get(11).mul(ilist.get(11)).sum() - sum(sorted4 :* sorted4)) < 1.0E-8)

    println("------------------------dense sparse-------------------------")

    //dense sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).mul(ilist.get(1))
      ilist.get(3).mul(ilist.get(4))
      ilist.get(6).mul(ilist.get(7))
      ilist.get(9).mul(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :* sparse1
      dense2 :* sparse2
      dense3 :* sparse3
      dense4 :* sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sparse mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).mul(ilist.get(1)).sum() - sum(dense1 :* sparse1)) < 1.0E-8)
    assert(abs(ilist.get(3).mul(ilist.get(4)).sum() - sum(dense2 :* sparse2)) < 1.0E-2)
    assert(abs(ilist.get(6).mul(ilist.get(7)).sum() - sum(dense3 :* sparse3)) < 1.0E-8)
    assert(abs(ilist.get(9).mul(ilist.get(10)).sum() - sum(dense4 :* sparse4)) < 1.0E-8)

    println("-------------------------------dense sorted-----------------------")

    //dense sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).mul(ilist.get(2))
      ilist.get(3).mul(ilist.get(5))
      ilist.get(6).mul(ilist.get(8))
      ilist.get(9).mul(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :* sorted1
      dense2 :* sorted2
      dense3 :* sorted3
      dense4 :* sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sorted mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(abs(ilist.get(0).mul(ilist.get(2)).sum() - sum(dense1 :* sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).mul(ilist.get(5)).sum() - sum(dense2 :* sorted2)) < 1.0E-2)
    assert(abs(ilist.get(6).mul(ilist.get(8)).sum() - sum(dense3 :* sorted3)) < 1.0E-8)
    assert(abs(ilist.get(9).mul(ilist.get(11)).sum() - sum(dense4 :* sorted4)) < 1.0E-8)

    println("------------------------sparse sorted------------------")

    //sparse sorted  cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).mul(ilist.get(2))
      ilist.get(4).mul(ilist.get(5))
      ilist.get(7).mul(ilist.get(8))
      ilist.get(10).mul(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :* sorted1
      sparse2 :* sorted2
      sparse3 :* sorted3
      sparse4 :* sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sorted mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    Thread.sleep(600L)
    assert(abs(ilist.get(1).mul(ilist.get(2)).sum() - sum(sparse1 :* sorted1)) < 1.0)
    assert(abs(ilist.get(4).mul(ilist.get(5)).sum() - sum(sparse2 :* sorted2)) < 1.0)
    assert(ilist.get(7).mul(ilist.get(8)).sum() == sum(sparse3 :* sorted3))
    assert(ilist.get(10).mul(ilist.get(11)).sum() == sum(sparse4 :* sorted4))
  }

  @Test
  def Divtest() {
    Thread.sleep(600L)
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).div(ilist.get(0))
      ilist.get(3).div(ilist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :/ dense1
      dense2 :/ dense2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    assert(ilist.get(0).div(ilist.get(0)).sum() == sum(dense1 :/ dense1))
    assert(abs(ilist.get(3).div(ilist.get(3)).sum() - sum(dense2 :/ dense2)) < 1.0E-8)

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).div(ilist.get(1))
      ilist.get(4).div(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :/ sparse1
      sparse2 :/ sparse2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(ilist.get(1).div(ilist.get(1)).sum().equals(sum(sparse1 :/ sparse1)))
    //    assert(ilist.get(4).div(ilist.get(4)).sum().equals(sum(sparse2 :/ sparse2)))

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).div(ilist.get(2))
      ilist.get(5).div(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :/ sorted1
      sorted2 :/ sorted2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(ilist.get(2).div(ilist.get(2)).sum().equals(sum(sorted1 :/ sorted1)))
    //    assert(ilist.get(5).div(ilist.get(5)).sum().equals(sum(sorted2 :/ sorted2)))

    println("------------------------dense sparse-------------------------")

    //dense sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).div(ilist.get(1))
      ilist.get(3).div(ilist.get(4))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :/ sparse1
      dense2 :/ sparse2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sparse div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(ilist.get(0).div(ilist.get(1)).sum().equals(sum(dense1 :/ sparse1)))
    //    assert(ilist.get(3).div(ilist.get(4)).sum().equals(sum(dense2 :/ sparse2)))

    println("-------------------------------dense sorted-----------------------")

    //dense sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).div(ilist.get(2))
      ilist.get(3).div(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1 :/ sorted1
      dense2 :/ sorted2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sorted div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(ilist.get(0).div(ilist.get(2)).sum().equals(sum(dense1 :/ sorted1)))
    //    assert(ilist.get(3).div(ilist.get(5)).sum().equals(sum(dense2 :/ sorted2)))

    println("------------------------sparse sorted------------------")

    //sparse sorted  cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).div(ilist.get(2))
      ilist.get(4).div(ilist.get(5))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :/ sorted1
      sparse2 :/ sorted2
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sorted div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //    assert(ilist.get(1).div(ilist.get(2)).sum().equals(sum(sparse1 :/ sorted1)))
    //    assert(ilist.get(4).div(ilist.get(5)).sum().equals(sum( sparse2 :/ sorted2)))
  }

  @Test
  def Axpytest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).axpy(ilist.get(0), 2.0)
      ilist.get(3).axpy(ilist.get(3), 2.0f)
      ilist.get(6).axpy(ilist.get(6), 2l)
      ilist.get(9).axpy(ilist.get(9), 2)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      axpy(2.0, dense1, dense1)
      axpy(2.0f, dense2, dense2)
      axpy(2L, dense3, dense3)
      axpy(2, dense4, dense4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense axpy:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).axpy(ilist.get(1), 2.0)
      ilist.get(4).axpy(ilist.get(4), 2.0f)
      ilist.get(7).axpy(ilist.get(7), 2l)
      ilist.get(10).axpy(ilist.get(10), 2)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      axpy(2.0, sparse1, sparse1)
      axpy(2.0f, sparse2, sparse2)
      axpy(2L, sparse3, sparse3)
      axpy(2, sparse4, sparse4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse axpy:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).axpy(ilist.get(2), 2.0)
      ilist.get(5).axpy(ilist.get(5), 2.0f)
      ilist.get(8).axpy(ilist.get(8), 2l)
      ilist.get(11).axpy(ilist.get(11), 2)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      axpy(2.0, sorted1, sorted1)
      axpy(2.0f, sorted2, sorted2)
      axpy(2L, sorted3, sorted3)
      axpy(2, sorted4, sorted4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted axpy:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

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
