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
import breeze.numerics.abs
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.Vector
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object DotCrossTest {
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

  var dummyValues = new Array[Int](capacity)
  var dummy1 = new SparseVector[Int](new SparseArray(intsortedIndices, dummyValues, capacity, dim, default = 0))

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

    dummyValues.indices.foreach { i =>
      dummyValues(i) = 1
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

    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))

    dummy1 = new SparseVector[Int](new SparseArray(intsortedIndices, dummyValues, capacity, dim, default = 0))

  }

}

class DotCrossTest {
  val capacity: Int = DotCrossTest.capacity
  val dim: Int = DotCrossTest.capacity * 100

  val ilist = DotCrossTest.ilist

  var sparse1 = DotCrossTest.sparse1
  var dense1 = DotCrossTest.dense1
  var sorted1 = DotCrossTest.sorted1

  var dense2 = DotCrossTest.dense2
  var sparse2 = DotCrossTest.sparse2
  var sorted2 = DotCrossTest.sorted2

  var dense3 = DotCrossTest.dense3
  var sparse3 = DotCrossTest.sparse3
  var sorted3 = DotCrossTest.sorted3

  var dense4 = DotCrossTest.dense4
  var sparse4 = DotCrossTest.sparse4
  var sorted4 = DotCrossTest.sorted4

  var dummyValues = DotCrossTest.dummyValues
  var dummy1 = DotCrossTest.dummy1

  @Test
  def CrossDottest() {

    assert(abs(ilist.get(0).dot(ilist.get(0)) - dense1.dot(dense1)) < 1.0E-8)
    assert(abs(ilist.get(3).dot(ilist.get(3)) - dense2.dot(dense2)) < 1.0)
    assert(abs(ilist.get(6).dot(ilist.get(6)) - dense3.dot(dense3)) < 1.0E-8)
    assert(abs(ilist.get(9).dot(ilist.get(9)) - dense4.dot(dense4)) < 1.0E-8)

    assert(abs(ilist.get(1).dot(ilist.get(1)) - sparse1.dot(sparse1)) < 1.0E-8)
    assert(abs(ilist.get(4).dot(ilist.get(4)) - sparse2.dot(sparse2)) < 1.0E-3)
    assert(abs(ilist.get(7).dot(ilist.get(7)) - sparse3.dot(sparse3)) < 1.0E-8)
    assert(abs(ilist.get(10).dot(ilist.get(10)) - sparse4.dot(sparse4)) < 1.0E-8)

    assert(abs(ilist.get(2).dot(ilist.get(2)) - sorted1.dot(sorted1)) < 1.0E-8)
    assert(abs(ilist.get(5).dot(ilist.get(5)) - sorted2.dot(sorted2)) < 1.0E-3)
    assert(abs(ilist.get(8).dot(ilist.get(8)) - sorted3.dot(sorted3)) < 1.0E-8)
    assert(abs(ilist.get(11).dot(ilist.get(11)) - sorted4.dot(sorted4)) < 1.0E-8)
    assert(abs(ilist.get(12).dot(ilist.get(12)) - dummyValues.length) < 1.0E-8)


    println("------------------------dense sparse-------------------------")

    assert(abs(ilist.get(0).dot(ilist.get(1)) - dense1.dot(sparse1)) < 1.0E-8)
    assert(abs(ilist.get(3).dot(ilist.get(4)) - dense2.dot(sparse2)) < 1.0E-3)
    assert(abs(ilist.get(6).dot(ilist.get(7)) - dense3.dot(sparse3)) < 1.0E-8)
    assert(abs(ilist.get(9).dot(ilist.get(10)) - dense4.dot(sparse4)) < 1.0E-8)


    println("-------------------------------dense sorted-----------------------")

    assert(abs(ilist.get(0).dot(ilist.get(2)) - dense1.dot(sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).dot(ilist.get(5)) - dense2.dot(sorted2)) < 1.0E-3)
    assert(abs(ilist.get(6).dot(ilist.get(8)) - dense3.dot(sorted3)) < 1.0E-8)
    assert(abs(ilist.get(9).dot(ilist.get(11)) - dense4.dot(sorted4)) < 1.0E-8)

    println("------------------------sparse sorted------------------")

    assert(abs(ilist.get(1).dot(ilist.get(2)) - sparse1.dot(sorted1)) < 1.0E-8)
    assert(abs(ilist.get(4).dot(ilist.get(5)) - sparse2.dot(sorted2)) < 1.0E-3)
    assert(abs(ilist.get(7).dot(ilist.get(8)) - sparse3.dot(sorted3)) < 1.0E-8)
    assert(abs(ilist.get(10).dot(ilist.get(11)) - sparse4.dot(sorted4)) < 1.0E-8)

    println("------------------dummy----------------------")

    assert(ilist.get(12).dot(ilist.get(12)) == dummy1.dot(dummy1))
    assert(ilist.get(9).dot(ilist.get(12)) == dense4.dot(dummy1))
    assert(ilist.get(12).dot(ilist.get(9)) == dummy1.dot(dense4))
    assert(ilist.get(10).dot(ilist.get(12)) == sparse4.dot(dummy1))
    assert(ilist.get(12).dot(ilist.get(10)) == dummy1.dot(sparse4))
    assert(ilist.get(11).dot(ilist.get(12)) == sorted4.dot(dummy1))
    assert(ilist.get(12).dot(ilist.get(11)) == dummy1.dot(sorted4))
  }
}
