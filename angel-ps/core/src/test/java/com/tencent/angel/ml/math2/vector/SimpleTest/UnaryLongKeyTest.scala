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
import breeze.linalg.{HashVector, SparseVector, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{Before, BeforeClass, Test}
import org.scalatest.FunSuite

object UnaryLongKeyTest {
  val capacity: Int = 1000
  val dim: Int = capacity * 100

  val intrandIndices: Array[Int] = new Array[Int](capacity)
  val longrandIndices: Array[Long] = new Array[Long](capacity)
  val intsortedIndices: Array[Int] = new Array[Int](capacity)
  val longsortedIndices: Array[Long] = new Array[Long](capacity)

  val floatValues: Array[Float] = new Array[Float](capacity)
  val doubleValues: Array[Double] = new Array[Double](capacity)

  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)

  val llist = new util.ArrayList[Vector]()

  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0F))


  val times = 500
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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

    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }
    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))
    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))
    sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse2(i) = v }
    sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0F))
  }

}

class UnaryLongKeyTest {
  val capacity: Int = UnaryLongKeyTest.capacity
  val dim: Int = UnaryLongKeyTest.dim

  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val llist = UnaryLongKeyTest.llist

  var sparse1 = UnaryLongKeyTest.sparse1
  var sorted1 = UnaryLongKeyTest.sorted1

  var sparse2 = UnaryLongKeyTest.sparse2
  var sorted2 = UnaryLongKeyTest.sorted2


  @Test
  def powTest() {

    assert(abs(Ufuncs.pow(llist.get(0), 2.0).sum() - sum(pow(sparse1, 2.0))) < 1.0)
    assert(abs(Ufuncs.pow(llist.get(1), 2.0).sum() - sum(pow(sorted1, 2.0))) < 1.0)
  }

  @Test
  def sqrtTest() {
    assert(abs(Ufuncs.sqrt(llist.get(0)).sum() - sum(sqrt(sparse1))) < 1.0)
    assert(Ufuncs.sqrt(llist.get(1)).sum() == sum(sqrt(sorted1)))

  }

  @Test
  def smulTest() {
    assert(abs(Ufuncs.smul(llist.get(0), 0.5).sum() - sum(sparse1 :* 0.5)) < 1.0)
    assert(Ufuncs.smul(llist.get(1), 0.5).sum() == sum(sorted1 :* 0.5))

  }

  @Test
  def sdivTest() {
    assert(abs(Ufuncs.sdiv(llist.get(0), 0.5).sum() - sum(sparse1 :/ 0.5)) < 1.0)
    assert(Ufuncs.sdiv(llist.get(1), 0.5).sum() == sum(sorted1 :/ 0.5))

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