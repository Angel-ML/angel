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
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.SimpleTest.BinaryIntKeyOPTest._
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}

object BinaryIntKeyOPTest {
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

  val doubledummyValues: Array[Double] = new Array[Double](capacity)
  val floatdummyValues: Array[Float] = new Array[Float](capacity)
  val longdummyValues: Array[Long] = new Array[Long](capacity)
  val intdummyValues: Array[Int] = new Array[Int](capacity)

  val denseintValues: Array[Int] = new Array[Int](dim)
  val denselongValues: Array[Long] = new Array[Long](dim)
  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)


  val ilist = new util.ArrayList[Vector]()
  val llist = new util.ArrayList[Vector]()

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

  var intdummy1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubledummyValues, capacity, dim, default = 0))
  var intdummy2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatdummyValues, capacity, dim, default = 0))
  var intdummy3 = new SparseVector[Long](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))
  var intdummy4 = new SparseVector[Int](new SparseArray(intsortedIndices, intdummyValues, capacity, dim, default = 0))

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
      longValues(i) = rand.nextInt(10) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(10) + 1
    }

    doubledummyValues.indices.foreach { i =>
      doubledummyValues(i) = 1.0
    }

    floatdummyValues.indices.foreach { i =>
      floatdummyValues(i) = 1.0f
    }

    longdummyValues.indices.foreach { i =>
      longdummyValues(i) = 1L
    }

    intdummyValues.indices.foreach { i =>
      intdummyValues(i) = 1
    }

    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(10) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(10) + 1
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
    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))

    dense4 = DenseVector[Int](denseintValues)
    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    intdummy1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubledummyValues, capacity, dim, default = 0))
    intdummy2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatdummyValues, capacity, dim, default = 0))
    intdummy3 = new SparseVector[Long](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))
    intdummy4 = new SparseVector[Int](new SparseArray(intsortedIndices, intdummyValues, capacity, dim, default = 0))
  }
}

class BinaryIntKeyOPTest {
  val ilist = BinaryIntKeyOPTest.ilist

  var sparse1 = BinaryIntKeyOPTest.sparse1
  var dense1 = BinaryIntKeyOPTest.dense1
  var sorted1 = BinaryIntKeyOPTest.sorted1

  var dense2 = BinaryIntKeyOPTest.dense2
  var sparse2 = BinaryIntKeyOPTest.sparse2
  var sorted2 = BinaryIntKeyOPTest.sorted2

  var dense3 = BinaryIntKeyOPTest.dense3
  var sparse3 = BinaryIntKeyOPTest.sparse3
  var sorted3 = BinaryIntKeyOPTest.sorted3

  var dense4 = BinaryIntKeyOPTest.dense4
  var sparse4 = BinaryIntKeyOPTest.sparse4
  var sorted4 = BinaryIntKeyOPTest.sorted4

  var intdummy1 = BinaryIntKeyOPTest.intdummy1
  var intdummy2 = BinaryIntKeyOPTest.intdummy2
  var intdummy3 = BinaryIntKeyOPTest.intdummy3
  var intdummy4 = BinaryIntKeyOPTest.intdummy4

  @Test
  def Addtest() {
    (0 until 3).foreach { i =>
      (0 until 3).foreach { j =>
        try {
          if (getFlag(ilist.get(j)) != "dummy") {
            assert(abs((ilist.get(i).add(ilist.get(j))).sum() - (ilist.get(i).sum() + ilist.get(j).sum())) < 1.0)
          } else {
            assert(abs((ilist.get(i).add(ilist.get(j))).sum() - (ilist.get(i).sum() + sum(intdummy1))) < 1.0)
          }
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }

  }

  @Test
  def Subtest() {
    (0 until 3).foreach { i =>
      (0 until 3).foreach { j =>
        try {
          if (getFlag(ilist.get(i)) != "dummy") {
            assert(abs((ilist.get(i).sub(ilist.get(j))).sum() - (ilist.get(i).sum() - ilist.get(j).sum())) < 1.0)
          } else {
            assert(abs((ilist.get(i).sub(ilist.get(j))).sum() - (ilist.get(i).sum() - sum(intdummy1))) < 1.0)
          }
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }

    assert((ilist.get(0).sub(ilist.get(0))).sum() == sum(dense1 - dense1))
    assert((ilist.get(1).sub(ilist.get(1))).sum() == sum(sparse1 - sparse1))
    assert((ilist.get(2).sub(ilist.get(2))).sum() == sum(sorted1 - sorted1))
    assert((ilist.get(3).sub(ilist.get(3))).sum() == sum(dense2 - dense2))
    assert((ilist.get(4).sub(ilist.get(4))).sum() == sum(sparse2 - sparse2))
    assert((ilist.get(5).sub(ilist.get(5))).sum() == sum(sorted2 - sorted2))
    assert((ilist.get(6).sub(ilist.get(6))).sum() == sum(dense3 - dense3))
    assert((ilist.get(7).sub(ilist.get(7))).sum() == sum(sparse3 - sparse3))
    assert((ilist.get(8).sub(ilist.get(8))).sum() == sum(sorted3 - sorted3))
    assert((ilist.get(9).sub(ilist.get(9))).sum() == sum(dense4 - dense4))
    assert((ilist.get(10).sub(ilist.get(10))).sum() == sum(sparse4 - sparse4))
    assert((ilist.get(11).sub(ilist.get(11))).sum() == sum(sorted4 - sorted4))

  }

  @Test
  def Multest() {
    (0 until 3).foreach { i =>
      (0 until 3).foreach { j =>
        try {
          ilist.get(i).mul(ilist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }

  }

  @Test
  def Divtest() {
    init()

    println(ilist.get(0).div(ilist.get(0)).sum(), sum(dense1 :/ dense1))
    println(ilist.get(1).div(ilist.get(1)).sum(), sum(sparse1 :/ sparse1))
    println(ilist.get(2).div(ilist.get(2)).sum(), sum(sorted1 :/ sorted1))
    println(ilist.get(3).div(ilist.get(3)).sum(), sum(dense2 :/ dense2))
    println(ilist.get(4).div(ilist.get(4)).sum(), sum(sparse2 :/ sparse2))
    println(ilist.get(5).div(ilist.get(5)).sum(), sum(sorted2 :/ sorted2))
    println(ilist.get(6).div(ilist.get(6)).sum(), sum(dense3 :/ dense3))

  }

  @Test
  def Axpytest() {
    init()
    (0 until 3).foreach { i =>
      (0 until 3).foreach { j =>
        try {
          ilist.get(i).axpy(ilist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
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