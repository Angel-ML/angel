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

import breeze.collection.mutable.{OpenAddressHashArray, SparseArray}
import breeze.linalg.{DenseVector, HashVector, SparseVector, sum}
import breeze.numerics.abs
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompSimpleBinaryIntkeyOPTest {
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

  val simpledenseintValues: Array[Int] = new Array[Int](dim * 3)
  val simpledenselongValues: Array[Long] = new Array[Long](dim * 3)
  val simpledensefloatValues: Array[Float] = new Array[Float](dim * 3)
  val simpledensedoubleValues: Array[Double] = new Array[Double](dim * 3)

  val simpleintrandIndices: Array[Int] = new Array[Int](capacity)
  val simplelongrandIndices: Array[Long] = new Array[Long](capacity)
  val simpleintsortedIndices: Array[Int] = new Array[Int](capacity)
  val simplelongsortedIndices: Array[Long] = new Array[Long](capacity)

  var intdummy = VFactory.intDummyVector(dim * 3, intValues)
  var longdummy = VFactory.longDummyVector(dim * 3, longValues)

  val list = new util.ArrayList[Vector]()
  val slist = new util.ArrayList[Vector]()

  var bsparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var bdense1 = DenseVector[Double](densedoubleValues)
  var bsorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var bdense2 = DenseVector[Float](densefloatValues)
  var bsparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var bsorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var bdense3 = DenseVector[Long](denselongValues)
  var bsparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var bsorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var bdense4 = DenseVector[Int](denseintValues)
  var bsparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var bsorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

  var doubleintdummy1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))
  var doubleintdummy2 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))
  var doubleintdummy3 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var floatintdummy1 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))
  var floatintdummy2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))
  var floatintdummy3 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var longintdummy1 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))
  var longintdummy2 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))
  var longintdummy3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))

  var intintdummy1 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))
  var intintdummy2 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))
  var intintdummy3 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

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
      longValues(i) = rand.nextInt(10)
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(10)
    }


    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(10)
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(10)
    }

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim * 3)
      if (!set.contains(t)) {
        simpleintrandIndices(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim * 2)
      if (!set.contains(t)) {
        simplelongrandIndices(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(simpleintrandIndices, 0, simpleintsortedIndices, 0, capacity)
    util.Arrays.sort(simpleintsortedIndices)

    System.arraycopy(simplelongrandIndices, 0, simplelongsortedIndices, 0, capacity)
    util.Arrays.sort(simplelongsortedIndices)

    simpledensedoubleValues.indices.foreach { i =>
      simpledensedoubleValues(i) = rand.nextDouble() + 0.01
    }

    simpledensefloatValues.indices.foreach { i =>
      simpledensefloatValues(i) = rand.nextFloat() + 0.01F
    }

    simpledenselongValues.indices.foreach { i =>
      simpledenselongValues(i) = rand.nextInt(10) + 1L
    }

    simpledenseintValues.indices.foreach { i =>
      simpledenseintValues(i) = rand.nextInt(10) + 1
    }


    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = new CompIntDoubleVector(dim * list1.length, list1)
    val simpledense1 = VFactory.denseDoubleVector(simpledensedoubleValues)
    val simplesparse1 = VFactory.sparseDoubleVector(dim * list1.length, simpleintrandIndices, doubleValues)
    val simplesorted1 = VFactory.sortedDoubleVector(dim * list1.length, simpleintsortedIndices, doubleValues)

    initIntDummy()

    list.add(comp1)
    slist.add(simpledense1)
    slist.add(simplesparse1)
    slist.add(simplesorted1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = new CompIntFloatVector(dim * list2.length, list2)
    val simpledense2 = VFactory.denseFloatVector(simpledensefloatValues)
    val simplesparse2 = VFactory.sparseFloatVector(dim * list1.length, simpleintrandIndices, floatValues)
    val simplesorted2 = VFactory.sortedFloatVector(dim * list1.length, simpleintsortedIndices, floatValues)

    list.add(comp2)
    slist.add(simpledense2)
    slist.add(simplesparse2)
    slist.add(simplesorted2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = new CompIntLongVector(dim * list3.length, list3)
    val simpledense3 = VFactory.denseLongVector(simpledenselongValues)
    val simplesparse3 = VFactory.sparseLongVector(dim * list1.length, simpleintrandIndices, longValues)
    val simplesorted3 = VFactory.sortedLongVector(dim * list1.length, simpleintsortedIndices, longValues)

    list.add(comp3)
    slist.add(simpledense3)
    slist.add(simplesparse3)
    slist.add(simplesorted3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = new CompIntIntVector(dim * list4.length, list4)
    val simpledense4 = VFactory.denseIntVector(simpledenseintValues)
    val simplesparse4 = VFactory.sparseIntVector(dim * list1.length, simpleintrandIndices, intValues)
    val simplesorted4 = VFactory.sortedIntVector(dim * list1.length, simpleintsortedIndices, intValues)

    intdummy = VFactory.intDummyVector(dim * list1.length, simpleintsortedIndices)

    list.add(comp4)
    slist.add(simpledense4)
    slist.add(simplesparse4)
    slist.add(simplesorted4)
    slist.add(intdummy)
  }

  def initIntDummy(): Unit = {
    val doubledummyValue1 = new Array[Double](simpleintsortedIndices.filter(_ < dim).length)
    doubledummyValue1.indices.foreach { i =>
      doubledummyValue1(i) = 1.0
    }
    val doubledummyValue2 = new Array[Double](simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length)
    doubledummyValue2.indices.foreach { i =>
      doubledummyValue2(i) = 1.0
    }
    val doubledummyValue3 = new Array[Double](simpleintsortedIndices.filter(_ >= 2 * dim).length)
    doubledummyValue3.indices.foreach { i =>
      doubledummyValue3(i) = 1.0
    }

    val floatdummyValue1 = new Array[Float](simpleintsortedIndices.filter(_ < dim).length)
    floatdummyValue1.indices.foreach { i =>
      floatdummyValue1(i) = 1.0f
    }
    val floatdummyValue2 = new Array[Float](simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length)
    floatdummyValue2.indices.foreach { i =>
      floatdummyValue2(i) = 1.0f
    }
    val floatdummyValue3 = new Array[Float](simpleintsortedIndices.filter(_ >= 2 * dim).length)
    floatdummyValue3.indices.foreach { i =>
      floatdummyValue3(i) = 1.0f
    }

    val longdummyValue1 = new Array[Long](simpleintsortedIndices.filter(_ < dim).length)
    longdummyValue1.indices.foreach { i =>
      longdummyValue1(i) = 1L
    }
    val longdummyValue2 = new Array[Long](simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length)
    longdummyValue2.indices.foreach { i =>
      longdummyValue2(i) = 1L
    }
    val longdummyValue3 = new Array[Long](simpleintsortedIndices.filter(_ >= 2 * dim).length)
    longdummyValue3.indices.foreach { i =>
      longdummyValue3(i) = 1L
    }

    val intdummyValue1 = new Array[Int](simpleintsortedIndices.filter(_ < dim).length)
    intdummyValue1.indices.foreach { i =>
      intdummyValue1(i) = 1
    }
    val intdummyValue2 = new Array[Int](simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length)
    intdummyValue2.indices.foreach { i =>
      intdummyValue2(i) = 1
    }
    val intdummyValue3 = new Array[Int](simpleintsortedIndices.filter(_ >= 2 * dim).length)
    intdummyValue3.indices.foreach { i =>
      intdummyValue3(i) = 1
    }

    bsparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => bsparse1(i) = v }
    bdense1 = DenseVector[Double](densedoubleValues)
    bsorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

    bdense2 = DenseVector[Float](densefloatValues)
    bsparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => bsparse2(i) = v }
    bsorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

    bdense3 = DenseVector[Long](denselongValues)
    bsparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => bsparse3(i) = v }
    bsorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

    bdense4 = DenseVector[Int](denseintValues)
    bsparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => bsparse4(i) = v }
    bsorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))


    doubleintdummy1 = new SparseVector[Double](new SparseArray(simpleintsortedIndices.filter(_ < dim), doubledummyValue1, simpleintsortedIndices.filter(_ < dim).length, dim, default = 0.0))
    doubleintdummy2 = new SparseVector[Double](new SparseArray(simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).map(i => i - dim), doubledummyValue2, simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length, dim, default = 0.0))
    doubleintdummy3 = new SparseVector[Double](new SparseArray(simpleintsortedIndices.filter(_ >= 2 * dim).map(i => i - 2 * dim), doubledummyValue3, simpleintsortedIndices.filter(_ >= 2 * dim).length, dim, default = 0.0))

    floatintdummy1 = new SparseVector[Float](new SparseArray(simpleintsortedIndices.filter(_ < dim), floatdummyValue1, simpleintsortedIndices.filter(_ < dim).length, dim, default = 0.0f))
    floatintdummy2 = new SparseVector[Float](new SparseArray(simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).map(i => i - dim), floatdummyValue2, simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length, dim, default = 0.0f))
    floatintdummy3 = new SparseVector[Float](new SparseArray(simpleintsortedIndices.filter(_ >= 2 * dim).map(i => i - 2 * dim), floatdummyValue3, simpleintsortedIndices.filter(_ >= 2 * dim).length, dim, default = 0.0f))

    longintdummy1 = new SparseVector[Long](new SparseArray(simpleintsortedIndices.filter(_ < dim), longdummyValue1, simpleintsortedIndices.filter(_ < dim).length, dim, default = 0L))
    longintdummy2 = new SparseVector[Long](new SparseArray(simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).map(i => i - dim), longdummyValue2, simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length, dim, default = 0L))
    longintdummy3 = new SparseVector[Long](new SparseArray(simpleintsortedIndices.filter(_ >= 2 * dim).map(i => i - 2 * dim), longdummyValue3, simpleintsortedIndices.filter(_ >= 2 * dim).length, dim, default = 0L))

    intintdummy1 = new SparseVector[Int](new SparseArray(simpleintsortedIndices.filter(_ < dim), intdummyValue1, simpleintsortedIndices.filter(_ < dim).length, dim, default = 0))
    intintdummy2 = new SparseVector[Int](new SparseArray(simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).map(i => i - dim), intdummyValue2, simpleintsortedIndices.filter(_ < 2 * dim).filter(_ >= dim).length, dim, default = 0))
    intintdummy3 = new SparseVector[Int](new SparseArray(simpleintsortedIndices.filter(_ >= 2 * dim).map(i => i - 2 * dim), intdummyValue3, simpleintsortedIndices.filter(_ >= 2 * dim).length, dim, default = 0))

  }
}

class CompSimpleBinaryIntkeyOPTest {
  val list = CompSimpleBinaryIntkeyOPTest.list
  val slist = CompSimpleBinaryIntkeyOPTest.slist
  var intdummy = CompSimpleBinaryIntkeyOPTest.intdummy

  var bdense1 = CompSimpleBinaryIntkeyOPTest.bdense1
  var bsparse1 = CompSimpleBinaryIntkeyOPTest.bsparse1
  var bsorted1 = CompSimpleBinaryIntkeyOPTest.bsorted1

  var bdense2 = CompSimpleBinaryIntkeyOPTest.bdense2
  var bsparse2 = CompSimpleBinaryIntkeyOPTest.bsparse2
  var bsorted2 = CompSimpleBinaryIntkeyOPTest.bsorted2

  var bdense3 = CompSimpleBinaryIntkeyOPTest.bdense3
  var bsparse3 = CompSimpleBinaryIntkeyOPTest.bsparse3
  var bsorted3 = CompSimpleBinaryIntkeyOPTest.bsorted3

  var bdense4 = CompSimpleBinaryIntkeyOPTest.bdense4
  var bsparse4 = CompSimpleBinaryIntkeyOPTest.bsparse4
  var bsorted4 = CompSimpleBinaryIntkeyOPTest.bsorted4

  var doubleintdummy1 = CompSimpleBinaryIntkeyOPTest.doubleintdummy1
  var doubleintdummy2 = CompSimpleBinaryIntkeyOPTest.doubleintdummy2
  var doubleintdummy3 = CompSimpleBinaryIntkeyOPTest.doubleintdummy3

  var floatintdummy1 = CompSimpleBinaryIntkeyOPTest.floatintdummy1
  var floatintdummy2 = CompSimpleBinaryIntkeyOPTest.floatintdummy2
  var floatintdummy3 = CompSimpleBinaryIntkeyOPTest.floatintdummy3

  var longintdummy1 = CompSimpleBinaryIntkeyOPTest.longintdummy1
  var longintdummy2 = CompSimpleBinaryIntkeyOPTest.longintdummy2
  var longintdummy3 = CompSimpleBinaryIntkeyOPTest.longintdummy3

  var intintdummy1 = CompSimpleBinaryIntkeyOPTest.intintdummy1
  var intintdummy2 = CompSimpleBinaryIntkeyOPTest.intintdummy2
  var intintdummy3 = CompSimpleBinaryIntkeyOPTest.intintdummy3

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def compAddsimpleTest() {
    //comp vs dense
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(slist.get(0))
      list.get(1).add(slist.get(3))
      list.get(2).add(slist.get(6))
      list.get(3).add(slist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs dense intkey add:$cost1")

    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(slist.get(1))
      list.get(1).add(slist.get(4))
      list.get(2).add(slist.get(7))
      list.get(3).add(slist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse intkey add:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(slist.get(2))
      list.get(1).add(slist.get(5))
      list.get(2).add(slist.get(8))
      list.get(3).add(slist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted intkey add:$cost1")


    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        if (getFlag(slist.get(j)) != "dummy") {
          assert(abs(list.get(i).add(slist.get(j)).sum() - (list.get(i).sum() + slist.get(j).sum())) < 1.0E-3)
        } else {
          assert(abs(list.get(i).add(slist.get(j)).sum() - (list.get(i).sum() + intdummy.sum())) < 1.0E-3)
        }
      }
    }
  }

  @Test
  def compSubsimpleTest() {
    //comp vs dense
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(slist.get(0))
      list.get(1).sub(slist.get(3))
      list.get(2).sub(slist.get(6))
      list.get(3).sub(slist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs dense intkey sub:$cost1")

    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(slist.get(1))
      list.get(1).sub(slist.get(4))
      list.get(2).sub(slist.get(7))
      list.get(3).sub(slist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse intkey sub:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(slist.get(2))
      list.get(1).sub(slist.get(5))
      list.get(2).sub(slist.get(8))
      list.get(3).sub(slist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted intkey sub:$cost1")

    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum()}, ${list.get(i).sum() - slist.get(j).sum()}, ${list.get(i).sub(slist.get(j)).sum()}")
        if (getFlag(slist.get(j)) != "dummy") {
          assert(abs(list.get(i).sub(slist.get(j)).sum() - (list.get(i).sum() - slist.get(j).sum())) < 1.0E-3)
        } else {
          assert(abs(list.get(i).sub(slist.get(j)).sum() - (list.get(i).sum() - intdummy.sum())) < 1.0E-3)
        }
      }
    }
  }

  @Test
  def compMulsimpleTest() {
    //comp vs dense
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(slist.get(0))
      list.get(1).mul(slist.get(3))
      list.get(2).mul(slist.get(6))
      list.get(3).mul(slist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs dense intkey mul:$cost1")

    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(slist.get(1))
      list.get(1).mul(slist.get(4))
      list.get(2).mul(slist.get(7))
      list.get(3).mul(slist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse intkey mul:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(slist.get(2))
      list.get(1).mul(slist.get(5))
      list.get(2).mul(slist.get(8))
      list.get(3).mul(slist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted intkey mul:$cost1")


    assert(abs(list.get(0).mul(intdummy).sum() - (sum(bdense1 :* doubleintdummy1) + sum(bsparse1 :* doubleintdummy2) + sum(bsorted1 :* doubleintdummy3))) < 1.0E-8)
    assert(abs(list.get(1).mul(intdummy).sum() - (sum(bdense2 :* floatintdummy1) + sum(bsparse2 :* floatintdummy2) + sum(bsorted2 :* floatintdummy3))) < 1.0E-3)
    assert(abs(list.get(2).mul(intdummy).sum() - (sum(bdense3 :* longintdummy1) + sum(bsparse3 :* longintdummy2) + sum(bsorted3 :* longintdummy3))) < 1.0E-8)
    assert(abs(list.get(3).mul(intdummy).sum() - (sum(bdense4 :* intintdummy1) + sum(bsparse4 :* intintdummy2) + sum(bsorted4 :* intintdummy3))) < 1.0E-8)

    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum()}, ${list.get(i).mul(slist.get(j)).sum()}")
      }
    }
  }

  @Test
  def compDivsimpleTest() {
    //comp vs dense
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).div(slist.get(0))
      list.get(1).div(slist.get(3))
      list.get(2).div(slist.get(6))
      list.get(3).div(slist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs dense intkey div:$cost1")

    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).div(slist.get(1))
      list.get(1).div(slist.get(4))
      //      list.get(2).div(slist.get(7))
      //      list.get(3).div(slist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse intkey div:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).div(slist.get(2))
      list.get(1).div(slist.get(5))
      //      list.get(2).div(slist.get(8))
      //      list.get(3).div(slist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted intkey div:$cost1")


    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum()}, ${list.get(i).div(slist.get(j)).sum()}")
      }
      println()
    }
  }

  @Test
  def compAxpysimpleTest() {
    //comp vs dense
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(slist.get(0), 2.0)
      list.get(1).axpy(slist.get(3), 2.0)
      list.get(2).axpy(slist.get(6), 2.0)
      list.get(3).axpy(slist.get(9), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs dense intkey axpy:$cost1")

    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(slist.get(1), 2.0)
      list.get(1).axpy(slist.get(4), 2.0)
      list.get(2).axpy(slist.get(7), 2.0)
      list.get(3).axpy(slist.get(10), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse intkey axpy:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(slist.get(2), 2.0)
      list.get(1).axpy(slist.get(5), 2.0)
      list.get(2).axpy(slist.get(8), 2.0)
      list.get(3).axpy(slist.get(11), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted intkey axpy:$cost1")

    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum() * 2}, ${list.get(i).axpy(slist.get(j), 2.0).sum()}")
        if (getFlag(slist.get(j)) != "dummy") {
          assert(abs(list.get(i).axpy(slist.get(j), 2.0).sum() - (list.get(i).sum() + slist.get(j).sum() * 2)) < 1.0E-3)
        } else {
          assert(abs(list.get(i).axpy(slist.get(j), 2.0).sum() - (list.get(i).sum() + intdummy.sum() * 2)) < 1.0E-3)
        }
      }
      println()
    }
  }

  @Test
  def compIaddsimpleTest() {
    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        assert(abs(list.get(i).iadd(slist.get(j)).sum() - (list.get(i).sum())) < 1.0E-8)
      }
    }
  }

  @Test
  def compIsubsimpleTest() {
    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        assert(abs(list.get(i).isub(slist.get(j)).sum() - (list.get(i).sum())) < 1.0E-8)
      }
    }
  }

  @Test
  def compImulsimpleTest() {
    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        assert(abs(list.get(i).imul(slist.get(j)).sum() - (list.get(i).sum())) < 1.0E-8)
      }
    }
  }

  @Test
  def compIdivsimpleTest() {
    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum()}, ${list.get(i).div(slist.get(j)).sum()}")
        assert(abs(list.get(i).idiv(slist.get(j)).sum() - (list.get(i).sum())) < 1.0E-8) //fail
      }
    }
  }

  @Test
  def compIaxpysimpleTest() {
    (0 until list.size()).foreach { i =>
      (i * 3 until slist.size()).foreach { j =>
        assert(abs(list.get(i).iaxpy(slist.get(j), 2.0).sum() - (list.get(i).sum())) < 1.0E-8)
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
