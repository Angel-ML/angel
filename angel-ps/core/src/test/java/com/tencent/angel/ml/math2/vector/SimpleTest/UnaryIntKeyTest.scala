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

    println("unary exp test")
    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} exp ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.exp(ilist.get(0)).sum()}, and breeze is ${sum(exp(dense1))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} exp ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.exp(ilist.get(1)).sum()}, and breeze is ${sum(exp(sparse1))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} exp ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.exp(ilist.get(2)).sum()}, and breeze is ${sum(exp(sorted1))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} exp ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.exp(ilist.get(3)).sum()}, and breeze is ${sum(exp(dense2))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} exp ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.exp(ilist.get(4)).sum()}, and breeze is ${sum(exp(sparse2))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} exp ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.exp(ilist.get(5)).sum()}, and breeze is ${sum(exp(sorted2))}")

    assert(Ufuncs.exp(ilist.get(0)).sum() == sum(exp(dense1)))
    assert(abs(Ufuncs.exp(ilist.get(1)).sum() - sum(exp(sparse1))) < 1.0E-8)
    assert(abs(Ufuncs.exp(ilist.get(2)).sum() - sum(exp(sorted1))) < 1.0E-8)
    assert(abs(Ufuncs.exp(ilist.get(3)).sum() - sum(exp(dense2))) < 1.0)
    assert(abs(Ufuncs.exp(ilist.get(4)).sum() - sum(exp(sparse2))) < 1.0E-1)
    assert(abs(Ufuncs.exp(ilist.get(5)).sum() - sum(exp(sorted2))) < 1.0)


    val idense1 = Ufuncs.iexp(ilist.get(0))
    val isparse1 = Ufuncs.iexp(ilist.get(1))
    val isorted1 = Ufuncs.iexp(ilist.get(2))
    val idense2 = Ufuncs.iexp(ilist.get(3))
    val isparse2 = Ufuncs.iexp(ilist.get(4))
    val isorted2 = Ufuncs.iexp(ilist.get(5))

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())
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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} log ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.log(ilist.get(0)).sum()}, and breeze is ${sum(log(dense1))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} log ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.log(ilist.get(1)).sum()}, and breeze is ${sum(log(sparse1))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} log ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.log(ilist.get(2)).sum()}, and breeze is ${sum(log(sorted1))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} log ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.log(ilist.get(3)).sum()}, and breeze is ${sum(log(dense2))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} log ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.log(ilist.get(4)).sum()}, and breeze is ${sum(log(sparse2))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} log ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.log(ilist.get(5)).sum()}, and breeze is ${sum(log(sorted2))}")

    assert(abs(Ufuncs.log(ilist.get(0)).sum() - sum(log(dense1))) < 1.0E-8)
    assert(Ufuncs.log(ilist.get(1)).sum() == sum(log(sparse1)))
    assert(Ufuncs.log(ilist.get(2)).sum() == sum(log(sorted1)))
    assert(abs(Ufuncs.log(ilist.get(3)).sum() - sum(log(dense2))) < 1.0)
    assert(Ufuncs.log(ilist.get(4)).sum() == sum(log(sparse2)))
    assert(Ufuncs.log(ilist.get(5)).sum() == sum(log(sorted2)))


    val idense1 = Ufuncs.ilog(ilist.get(0))
    val isparse1 = Ufuncs.ilog(ilist.get(1))
    val isorted1 = Ufuncs.ilog(ilist.get(2))
    val idense2 = Ufuncs.ilog(ilist.get(3))
    val isparse2 = Ufuncs.ilog(ilist.get(4))
    val isorted2 = Ufuncs.ilog(ilist.get(5))

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} log1p ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.log1p(ilist.get(0)).sum()}, and breeze is ${sum(log1p(dense1))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} log1p ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.log1p(ilist.get(1)).sum()}, and breeze is ${sum(log1p(sparse1))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} log1p ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.log1p(ilist.get(2)).sum()}, and breeze is ${sum(log1p(sorted1))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} log1p ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.log1p(ilist.get(3)).sum()}, and breeze is ${sum(log1p(dense2))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} log1p ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.log1p(ilist.get(4)).sum()}, and breeze is ${sum(log1p(sparse2))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} log1p ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.log1p(ilist.get(5)).sum()}, and breeze is ${sum(log1p(sorted2))}")

    assert(abs(Ufuncs.log1p(ilist.get(0)).sum() - sum(log1p(dense1))) < 1.0E-8)
    assert(abs(Ufuncs.log1p(ilist.get(1)).sum() - sum(log1p(sparse1))) < 1.0E-8)
    assert(abs(Ufuncs.log1p(ilist.get(2)).sum() - sum(log1p(sorted1))) < 1.0)
    assert(abs(Ufuncs.log1p(ilist.get(3)).sum() - sum(log1p(dense2))) < 1.0)
    assert(abs(Ufuncs.log1p(ilist.get(4)).sum() - sum(log1p(sparse2))) < 1.0E-3)
    assert(abs(Ufuncs.log1p(ilist.get(5)).sum() - sum(log1p(sorted2))) < 1.0)


    val idense1 = Ufuncs.ilog1p(ilist.get(0))
    val isparse1 = Ufuncs.ilog1p(ilist.get(1))
    val isorted1 = Ufuncs.ilog1p(ilist.get(2))
    val idense2 = Ufuncs.ilog1p(ilist.get(3))
    val isparse2 = Ufuncs.ilog1p(ilist.get(4))
    val isorted2 = Ufuncs.ilog1p(ilist.get(5))

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} sigmoid ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${TransFuncs.sigmoid(ilist.get(0)).sum()}, and breeze is ${sum(sigmoid(dense1))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} sigmoid ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${TransFuncs.sigmoid(ilist.get(1)).sum()}, and breeze is ${sum(sigmoid(sparse1))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} sigmoid ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${TransFuncs.sigmoid(ilist.get(2)).sum()}, and breeze is ${sum(sigmoid(sorted1))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} sigmoid ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${TransFuncs.sigmoid(ilist.get(3)).sum()}, and breeze is ${sum(sigmoid(dense2))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} sigmoid ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${TransFuncs.sigmoid(ilist.get(4)).sum()}, and breeze is ${sum(sigmoid(sparse2))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} sigmoid ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${TransFuncs.sigmoid(ilist.get(5)).sum()}, and breeze is ${sum(sigmoid(sorted2))}")

    assert(abs(TransFuncs.sigmoid(ilist.get(0)).sum() - sum(sigmoid(dense1))) < 1.0E-8)
    assert(abs(TransFuncs.sigmoid(ilist.get(1)).sum() - sum(sigmoid(sparse1))) < 1.0E-8)
    assert(abs(TransFuncs.sigmoid(ilist.get(2)).sum() - sum(sigmoid(sorted1))) < 1.0)
    assert(abs(TransFuncs.sigmoid(ilist.get(3)).sum() - sum(sigmoid(dense2))) < 1.0)
    assert(abs(TransFuncs.sigmoid(ilist.get(4)).sum() - sum(sigmoid(sparse2))) < 1.0E-1)
    assert(abs(TransFuncs.sigmoid(ilist.get(5)).sum() - sum(sigmoid(sorted2))) < 1.0)

    val idense1 = TransFuncs.isigmoid(ilist.get(0))
    val isparse1 = TransFuncs.isigmoid(ilist.get(1))
    val isorted1 = TransFuncs.isigmoid(ilist.get(2))
    val idense2 = TransFuncs.isigmoid(ilist.get(3))
    val isparse2 = TransFuncs.isigmoid(ilist.get(4))
    val isorted2 = TransFuncs.isigmoid(ilist.get(5))

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} softthreshold ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.softthreshold(ilist.get(0), 1.5).sum()}, and breeze is ${sum(sin(dense1) :* d1)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} softthreshold ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.softthreshold(ilist.get(1), 1.5).sum()}, and breeze is ${sum(sin(sparse1) :* d1)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} softthreshold ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.softthreshold(ilist.get(2), 1.5).sum()}, and breeze is ${sum(sin(sorted1) :* d1)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} softthreshold ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.softthreshold(ilist.get(3), 1.5f).sum()}, and breeze is ${sum(sin(dense2) :* d2)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} softthreshold ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.softthreshold(ilist.get(4), 1.5f).sum()}, and breeze is ${sum(sin(sparse2) :* d2)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} softthreshold ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.softthreshold(ilist.get(5), 1.5f).sum()}, and breeze is ${sum(sin(sorted2) :* d2)}")

    assert(abs(Ufuncs.softthreshold(ilist.get(0), 1.5).sum() - sum(sin(dense1) :* d1)) < 1.0E-8)
    assert(abs(Ufuncs.softthreshold(ilist.get(1), 1.5).sum() - sum(sin(sparse1) :* d1)) < 1.0E-8)
    assert(abs(Ufuncs.softthreshold(ilist.get(2), 1.5).sum() - sum(sin(sorted1) :* d1)) < 1.0E-8)
    assert(abs(Ufuncs.softthreshold(ilist.get(3), 1.5f).sum() - sum(sin(dense2) :* d2)) < 1.0E-8)
    assert(abs(Ufuncs.softthreshold(ilist.get(4), 1.5f).sum() - sum(sin(sparse2) :* d2)) < 1.0E-8)
    assert(abs(Ufuncs.softthreshold(ilist.get(5), 1.5f).sum() - sum(sin(sorted2) :* d2)) < 1.0E-8)

    val idense1 = Ufuncs.isoftthreshold(ilist.get(0), 1.5)
    val isparse1 = Ufuncs.isoftthreshold(ilist.get(1), 1.5)
    val isorted1 = Ufuncs.isoftthreshold(ilist.get(2), 1.5)
    val idense2 = Ufuncs.isoftthreshold(ilist.get(3), 1.5f)
    val isparse2 = Ufuncs.isoftthreshold(ilist.get(4), 1.5f)
    val isorted2 = Ufuncs.isoftthreshold(ilist.get(5), 1.5f)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} sadd ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.sadd(ilist.get(0), 0.5).sum()}, and breeze is ${sum(dense1 + 0.5)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} sadd ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} ${ilist.get(1).sum()}, and breeze is ${sum(sparse1)}, is ${Ufuncs.sadd(ilist.get(1), 0.5).sum()}, and breeze is ${sum(sparse1 + 0.5)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} sadd ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} ${ilist.get(2).sum()}, and breeze is ${sum(sorted1)}, is ${Ufuncs.sadd(ilist.get(2), 0.5).sum()}, and breeze is ${sum(sorted1 + 0.5)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} sadd ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.sadd(ilist.get(3), 0.5).sum()}, and breeze is ${sum(dense2 + 0.5f)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} sadd ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.sadd(ilist.get(4), 0.5).sum()}, and breeze is ${sum(sparse2 + 0.5f)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} sadd ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.sadd(ilist.get(5), 0.5).sum()}, and breeze is ${sum(sorted2 + 0.5f)}")

    assert(Ufuncs.sadd(ilist.get(0), 0.5).sum() == sum(dense1 + 0.5))
    assert(abs(Ufuncs.sadd(ilist.get(1), 0.5).sum() - sum(sparse1 + 0.5)) < 1.0E-8)
    assert(abs(Ufuncs.sadd(ilist.get(2), 0.5).sum() - sum(sorted1 + 0.5)) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(3), 0.5).sum() - sum(dense2 + 0.5f)) < 1.0)
    assert(abs(Ufuncs.sadd(ilist.get(4), 0.5).sum() - sum(sparse2 + 0.5f)) < 1.0E-1)
    assert(abs(Ufuncs.sadd(ilist.get(5), 0.5).sum() - sum(sorted2 + 0.5f)) < 1.0)
    assert(Ufuncs.sadd(ilist.get(6), 2).sum() == sum((dense3 + 2L)))
    assert(abs(Ufuncs.sadd(ilist.get(7), 2).sum() - sum((sparse3 + 2L))) < 1.0E-8)
    println(ilist.get(8).sum(), sum(sorted3), Ufuncs.sadd(ilist.get(8), 2).sum(), sum(sorted3 + 2L), ilist.get(8).sum(), sum(sorted3))
    //    assert(Ufuncs.sadd(ilist.get(8),2.0).sum()== sum((sorted3+2L)))
    assert(abs(Ufuncs.sadd(ilist.get(9), 2).sum() - sum((dense4 + 2))) < 1.0E-8)
    println(ilist.get(10).sum(), sum(sparse4), Ufuncs.sadd(ilist.get(10), 2.0).sum(), sum(sparse4 + 2), ilist.get(10).sum(), sum(sparse4))
    assert(abs(Ufuncs.sadd(ilist.get(10), 2).sum() - sum((sparse4 + 2))) < 1.0E-3)
    println(ilist.get(11).sum(), sum(sorted4), Ufuncs.sadd(ilist.get(11), 2.0).sum(), sum(sorted4 + 2), ilist.get(11).sum(), sum(sorted4))
    //    assert(abs(Ufuncs.sadd(ilist.get(11),2).sum()- sum(sorted4+2)) < 1.0E-3)

    val idense1 = Ufuncs.isadd(ilist.get(0), 0.5)
    val isparse1 = Ufuncs.isadd(ilist.get(1), 0.5)
    val isorted1 = Ufuncs.isadd(ilist.get(2), 0.5)
    val idense2 = Ufuncs.isadd(ilist.get(3), 0.5)
    val isparse2 = Ufuncs.isadd(ilist.get(4), 0.5)
    val isorted2 = Ufuncs.isadd(ilist.get(5), 0.5)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} ssub ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.ssub(ilist.get(0), 0.5).sum()}, and breeze is ${sum(dense1 - 0.5)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} ssub ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} ${ilist.get(1).sum()}, and breeze is ${sum(sparse1)}, is ${Ufuncs.ssub(ilist.get(1), 0.5).sum()}, and breeze is ${sum(sparse1 - 0.5)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} ssub ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} ${ilist.get(2).sum()}, and breeze is ${sum(sorted1)}, is ${Ufuncs.ssub(ilist.get(2), 0.5).sum()}, and breeze is ${sum(sorted1 - 0.5)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} ssub ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.ssub(ilist.get(3), 0.5f).sum()}, and breeze is ${sum(dense2 - 0.5f)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} ssub ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.ssub(ilist.get(4), 0.5f).sum()}, and breeze is ${sum(sparse2 - 0.5f)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} ssub ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.ssub(ilist.get(5), 0.5f).sum()}, and breeze is ${sum(sorted2 - 0.5f)}")

    assert(abs(Ufuncs.ssub(ilist.get(0), 0.5).sum() - sum(dense1 - 0.5)) < 1.0E-8)
    assert(abs(Ufuncs.ssub(ilist.get(1), 0.5).sum() - sum(sparse1 - 0.5)) < 1.0E-8)
    assert(abs(Ufuncs.ssub(ilist.get(2), 0.5).sum() - sum(sorted1 - 0.5)) < 1.0)
    assert(abs(Ufuncs.ssub(ilist.get(3), 0.5f).sum() - sum(dense2 - 0.5f)) < 1.0E-1)
    assert(abs(Ufuncs.ssub(ilist.get(4), 0.5f).sum() - sum(sparse2 - 0.5f)) < 1.0E-1)
    assert(abs(Ufuncs.ssub(ilist.get(5), 0.5f).sum() - sum(sorted2 - 0.5f)) < 1.0)

    val idense1 = Ufuncs.issub(ilist.get(0), 0.5)
    val isparse1 = Ufuncs.issub(ilist.get(1), 0.5)
    val isorted1 = Ufuncs.issub(ilist.get(2), 0.5)
    val idense2 = Ufuncs.issub(ilist.get(3), 0.5f)
    val isparse2 = Ufuncs.issub(ilist.get(4), 0.5f)
    val isorted2 = Ufuncs.issub(ilist.get(5), 0.5f)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} pow ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.pow(ilist.get(0), 2.0).sum()}, and breeze is ${sum(pow(dense1, 2.0))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} pow ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.pow(ilist.get(1), 2.0).sum()}, and breeze is ${sum(pow(sparse1, 2.0))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} pow ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.pow(ilist.get(2), 2.0).sum()}, and breeze is ${sum(pow(sorted1, 2.0))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} pow ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.pow(ilist.get(3), 2.0f).sum()}, and breeze is ${sum(pow(dense2, 2.0f))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} pow ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.pow(ilist.get(4), 2.0f).sum()}, and breeze is ${sum(pow(sparse2, 2.0f))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} pow ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.pow(ilist.get(5), 2.0f).sum()}, and breeze is ${sum(pow(sorted2, 2.0f))}")

    assert(Ufuncs.pow(ilist.get(0), 2.0).sum() == sum(pow(dense1, 2.0)))
    assert(abs(Ufuncs.pow(ilist.get(1), 2.0).sum() - sum(pow(sparse1, 2.0))) < 1.0E-8)
    assert(Ufuncs.pow(ilist.get(2), 2.0).sum() == sum(pow(sorted1, 2.0)))
    assert(abs(Ufuncs.pow(ilist.get(3), 2.0f).sum() - sum(pow(dense2, 2.0f))) < 1)
    assert(abs(Ufuncs.pow(ilist.get(4), 2.0f).sum() - sum(pow(sparse2, 2.0f))) < 1.0E-3)
    assert(abs(Ufuncs.pow(ilist.get(5), 2.0f).sum() - sum(pow(sorted2, 2.0f))) < 1.0E-3)

    val idense1 = Ufuncs.ipow(ilist.get(0), 2.0)
    val isparse1 = Ufuncs.ipow(ilist.get(1), 2.0)
    val isorted1 = Ufuncs.ipow(ilist.get(2), 2.0)
    val idense2 = Ufuncs.ipow(ilist.get(3), 2.0f)
    val isparse2 = Ufuncs.ipow(ilist.get(4), 2.0f)
    val isorted2 = Ufuncs.ipow(ilist.get(5), 2.0f)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())
  }

  @Test
  def sqrtTest() {
    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(0))
      Ufuncs.sqrt(ilist.get(3))
      Ufuncs.sqrt(ilist.get(6))
      Ufuncs.sqrt(ilist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(dense1)
      sqrt(dense2)
      sqrt(dense3)
      sqrt(dense4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(1))
      Ufuncs.sqrt(ilist.get(4))
      Ufuncs.sqrt(ilist.get(7))
      Ufuncs.sqrt(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sparse1)
      sqrt(sparse2)
      sqrt(sparse3)
      sqrt(sparse4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(ilist.get(2))
      Ufuncs.sqrt(ilist.get(5))
      Ufuncs.sqrt(ilist.get(8))
      Ufuncs.sqrt(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sorted1)
      sqrt(sorted2)
      sqrt(sorted3)
      sqrt(sorted4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} sqrt ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.sqrt(ilist.get(0)).sum()}, and breeze is ${sum(sqrt(dense1))}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} sqrt ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.sqrt(ilist.get(1)).sum()}, and breeze is ${sum(sqrt(sparse1))}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} sqrt ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.sqrt(ilist.get(2)).sum()}, and breeze is ${sum(sqrt(sorted1))}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} sqrt ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.sqrt(ilist.get(3)).sum()}, and breeze is ${sum(sqrt(dense2))}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} sqrt ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.sqrt(ilist.get(4)).sum()}, and breeze is ${sum(sqrt(sparse2))}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} sqrt ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.sqrt(ilist.get(5)).sum()}, and breeze is ${sum(sqrt(sorted2))}")

    assert(Ufuncs.sqrt(ilist.get(0)).sum() == sum(sqrt(dense1)))
    assert(abs(Ufuncs.sqrt(ilist.get(1)).sum() - sum(sqrt(sparse1))) < 1.0E-8)
    assert(Ufuncs.sqrt(ilist.get(2)).sum() == sum(sqrt(sorted1)))
    assert(abs(Ufuncs.sqrt(ilist.get(3)).sum() - sum(sqrt(dense2))) < 1.0)
    assert(abs(Ufuncs.sqrt(ilist.get(4)).sum() - sum(sqrt(sparse2))) < 1.0E-3)
    assert(abs(Ufuncs.sqrt(ilist.get(5)).sum() - sum(sqrt(sorted2))) < 1.0E-3)

    println(s"${ilist.get(6).sum()},${sum(dense3)},${Ufuncs.sqrt(ilist.get(6)).sum()}, ${sum(sqrt(dense3))}")
    println(s"${ilist.get(7).sum()},${sum(sparse3)},${Ufuncs.sqrt(ilist.get(7)).sum()}, ${sum(sqrt(sparse3))}")
    println(s"${ilist.get(8).sum()},${sum(sorted3)},${Ufuncs.sqrt(ilist.get(8)).sum()}, ${sum(sqrt(sorted3))}")
    println(s"${ilist.get(9).sum()},${sum(dense4)},${Ufuncs.sqrt(ilist.get(9)).sum()}, ${sum(sqrt(dense4))}")
    println(s"${ilist.get(10).sum()},${sum(sparse4)},${Ufuncs.sqrt(ilist.get(10)).sum()}, ${sum(sqrt(sparse4))}")
    println(s"${ilist.get(11).sum()},${sum(sorted4)},${Ufuncs.sqrt(ilist.get(11)).sum()}, ${sum(sqrt(sorted4))}")
    //    assert(Ufuncs.sqrt(ilist.get(6)).sum()== sum(sqrt(dense3)))
    //    assert(abs(Ufuncs.sqrt(ilist.get(7)).sum()- sum(sqrt(sparse3))) < 1.0E-8)
    //    assert(Ufuncs.sqrt(ilist.get(8)).sum()== sum(sqrt(sorted3)))
    //    assert(abs(Ufuncs.sqrt(ilist.get(9)).sum()- sum(sqrt(dense4))) < 1.0)
    //    assert(abs(Ufuncs.sqrt(ilist.get(10)).sum()- sum(sqrt(sparse4))) < 1.0E-3)
    //    assert(abs(Ufuncs.sqrt(ilist.get(11)).sum()- sum(sqrt(sorted4))) < 1.0E-3)

    val idense1 = Ufuncs.isqrt(ilist.get(0))
    val isparse1 = Ufuncs.isqrt(ilist.get(1))
    val isorted1 = Ufuncs.isqrt(ilist.get(2))
    val idense2 = Ufuncs.isqrt(ilist.get(3))
    val isparse2 = Ufuncs.isqrt(ilist.get(4))
    val isorted2 = Ufuncs.isqrt(ilist.get(5))

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} smul ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.smul(ilist.get(0), 0.5).sum()}, and breeze is ${sum(dense1 :* 0.5)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} smul ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.smul(ilist.get(1), 0.5).sum()}, and breeze is ${sum(sparse1 :* 0.5)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} smul ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.smul(ilist.get(2), 0.5).sum()}, and breeze is ${sum(sorted1 :* 0.5)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} smul ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.smul(ilist.get(3), 0.5f).sum()}, and breeze is ${sum(dense2 :* 0.5f)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} smul ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.smul(ilist.get(4), 0.5f).sum()}, and breeze is ${sum(sparse2 :* 0.5f)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} smul ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.smul(ilist.get(5), 0.5f).sum()}, and breeze is ${sum(sorted2 :* 0.5f)}")

    assert(Ufuncs.smul(ilist.get(0), 0.5).sum() == sum(dense1 :* 0.5))
    assert(abs(Ufuncs.smul(ilist.get(1), 0.5).sum() - sum(sparse1 :* 0.5)) < 1.0E-8)
    assert(Ufuncs.smul(ilist.get(2), 0.5).sum() == sum(sorted1 :* 0.5))
    assert(abs(Ufuncs.smul(ilist.get(3), 0.5f).sum() - sum(dense2 :* 0.5f)) < 1.0)
    assert(abs(Ufuncs.smul(ilist.get(4), 0.5f).sum() - sum(sparse2 :* 0.5f)) < 1.0E-3)
    assert(abs(Ufuncs.smul(ilist.get(5), 0.5f).sum() - sum(sorted2 :* 0.5f)) < 1.0E-3)

    println(s"${ilist.get(6).sum()},${sum(dense3)},${Ufuncs.smul(ilist.get(6), 5).sum()}, ${sum(dense3 :* 5L)}")
    println(s"${ilist.get(7).sum()},${sum(sparse3)},${Ufuncs.smul(ilist.get(7), 5).sum()}, ${sum(sparse3 :* 5L)}")
    println(s"${ilist.get(8).sum()},${sum(sorted3)},${Ufuncs.smul(ilist.get(8), 5).sum()}, ${sum(sorted3 :* 5L)}")
    println(s"${ilist.get(9).sum()},${sum(dense4)},${Ufuncs.smul(ilist.get(9), 5).sum()}, ${sum(dense4 :* 5)}")
    println(s"${ilist.get(10).sum()},${sum(sparse4)},${Ufuncs.smul(ilist.get(10), 5).sum()}, ${sum(sparse4 :* 5)}")
    println(s"${ilist.get(11).sum()},${sum(sorted4)},${Ufuncs.smul(ilist.get(11), 5).sum()}, ${sum(sorted4 :* 5)}")
    assert(Ufuncs.smul(ilist.get(6), 5).sum() == sum(dense3 :* 5L))
    assert(abs(Ufuncs.smul(ilist.get(7), 5).sum() - sum(sparse3 :* 5L)) < 1.0E-8)
    assert(Ufuncs.smul(ilist.get(8), 5).sum() == sum(sorted3 :* 5L))
    assert(abs(Ufuncs.smul(ilist.get(9), 5).sum() - sum(dense4 :* 5)) < 1.0)
    assert(abs(Ufuncs.smul(ilist.get(10), 5).sum() - sum(sparse4 :* 5)) < 1.0E-3)
    assert(abs(Ufuncs.smul(ilist.get(11), 5).sum() - sum(sorted4 :* 5)) < 1.0E-3)

    val idense1 = Ufuncs.ismul(ilist.get(0), 0.5)
    val isparse1 = Ufuncs.ismul(ilist.get(1), 0.5)
    val isorted1 = Ufuncs.ismul(ilist.get(2), 0.5)
    val idense2 = Ufuncs.ismul(ilist.get(3), 0.5f)
    val isparse2 = Ufuncs.ismul(ilist.get(4), 0.5f)
    val isorted2 = Ufuncs.ismul(ilist.get(5), 0.5f)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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


    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} sdiv ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${Ufuncs.sdiv(ilist.get(0), 0.5).sum()}, and breeze is ${sum(dense1 :/ 0.5)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} sdiv ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${Ufuncs.sdiv(ilist.get(1), 0.5).sum()}, and breeze is ${sum(sparse1 :/ 0.5)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} sdiv ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${Ufuncs.sdiv(ilist.get(2), 0.5).sum()}, and breeze is ${sum(sorted1 :/ 0.5)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} sdiv ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${Ufuncs.sdiv(ilist.get(3), 0.5f).sum()}, and breeze is ${sum(dense2 :/ 0.5f)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} sdiv ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${Ufuncs.sdiv(ilist.get(4), 0.5f).sum()}, and breeze is ${sum(sparse2 :/ 0.5f)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} sdiv ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${Ufuncs.sdiv(ilist.get(5), 0.5f).sum()}, and breeze is ${sum(sorted2 :/ 0.5f)}")

    assert(Ufuncs.sdiv(ilist.get(0), 0.5).sum() == sum(dense1 :/ 0.5))
    assert(abs(Ufuncs.sdiv(ilist.get(1), 0.5).sum() - sum(sparse1 :/ 0.5)) < 1.0E-8)
    assert(Ufuncs.sdiv(ilist.get(2), 0.5).sum() == sum(sorted1 :/ 0.5))
    assert(abs(Ufuncs.sdiv(ilist.get(3), 0.5f).sum() - sum(dense2 :/ 0.5f)) < 1.0)
    assert(abs(Ufuncs.sdiv(ilist.get(4), 0.5f).sum() - sum(sparse2 :/ 0.5f)) < 1.0E-3)
    assert(abs(Ufuncs.sdiv(ilist.get(5), 0.5f).sum() - sum(sorted2 :/ 0.5f)) < 1.0E-3)

    val idense1 = Ufuncs.isdiv(ilist.get(0), 0.5)
    val isparse1 = Ufuncs.isdiv(ilist.get(1), 0.5)
    val isorted1 = Ufuncs.isdiv(ilist.get(2), 0.5)
    val idense2 = Ufuncs.isdiv(ilist.get(3), 0.5f)
    val isparse2 = Ufuncs.isdiv(ilist.get(4), 0.5f)
    val isorted2 = Ufuncs.isdiv(ilist.get(5), 0.5f)

    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())

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
