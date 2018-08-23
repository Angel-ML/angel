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


package com.tencent.angel.ml.math2.vector.CompTest

import java.util

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompReduceCostTest {
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

  val list1 = new Array[IntDoubleVector](3)
  val list2 = new Array[IntFloatVector](3)
  val list3 = new Array[IntLongVector](3)
  val list4 = new Array[IntIntVector](3)

  var comp1 = new CompIntDoubleVector(dim * 3, list1)
  var comp2 = new CompIntFloatVector(dim * 3, list2)
  var comp3 = new CompIntLongVector(dim * 3, list3)
  var comp4 = new CompIntIntVector(dim * 3, list4)

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

    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    comp1 = new CompIntDoubleVector(dim * list1.length, list1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    comp2 = new CompIntFloatVector(dim * list2.length, list2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    comp3 = new CompIntLongVector(dim * list3.length, list3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    comp4 = new CompIntIntVector(dim * list4.length, list4)
  }
}

class CompReduceCostTest {
  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  var comp1 = CompReduceCostTest.comp1
  var comp2 = CompReduceCostTest.comp2
  var comp3 = CompReduceCostTest.comp3
  var comp4 = CompReduceCostTest.comp4

  @Test
  def sumCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.sum()
      comp2.sum()
      comp3.sum()
      comp4.sum()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp sum:$cost1")
  }

  @Test
  def averageCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.average()
      comp2.average()
      comp3.average()
      comp4.average()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp average:$cost1")
  }

  @Test
  def maxCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.max()
      comp2.max()
      comp3.max()
      comp4.max()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp max:$cost1")
  }

  @Test
  def minCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.min()
      comp2.min()
      comp3.min()
      comp4.min()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp min:$cost1")
  }

  @Test
  def stdCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.std()
      comp2.std()
      comp3.std()
      comp4.std()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp std:$cost1")
  }

  @Test
  def normCostTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      comp1.norm()
      comp2.norm()
      comp3.norm()
      comp4.norm()
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp norm:$cost1")
  }
}
