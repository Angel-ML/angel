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

import breeze.numerics.abs
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompSimpleBinaryLongkeyOPTest {
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

  var longdummy = VFactory.longDummyVector(dim * 2, simplelongsortedIndices)

  val list = new util.ArrayList[Vector]()
  val slist = new util.ArrayList[Vector]()

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
      floatValues(i) = rand.nextFloat() + 0.01F
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(10) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(10) + 1
    }


    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble() + 0.01
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat() + 0.01F
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(10) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(10) + 1
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


    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1 = Array(sparse1, sorted1)
    val comp1 = new CompLongDoubleVector(dim * list1.length, list1)
    val simplesparse1 = VFactory.sparseLongKeyDoubleVector(dim * list1.length, simplelongrandIndices, doubleValues)
    val simplesorted1 = VFactory.sortedLongKeyDoubleVector(dim * list1.length, simplelongsortedIndices, doubleValues)

    list.add(comp1)
    slist.add(simplesparse1)
    slist.add(simplesorted1)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2 = Array(sparse2, sorted2)
    val comp2 = new CompLongFloatVector(dim * list2.length, list2)
    val simplesparse2 = VFactory.sparseLongKeyFloatVector(dim * list1.length, simplelongrandIndices, floatValues)
    val simplesorted2 = VFactory.sortedLongKeyFloatVector(dim * list1.length, simplelongsortedIndices, floatValues)

    list.add(comp2)
    slist.add(simplesparse2)
    slist.add(simplesorted2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3 = Array(sparse3, sorted3)
    val comp3 = new CompLongLongVector(dim * list3.length, list3)
    val simplesparse3 = VFactory.sparseLongKeyLongVector(dim * list1.length, simplelongrandIndices, longValues)
    val simplesorted3 = VFactory.sortedLongKeyLongVector(dim * list1.length, simplelongsortedIndices, longValues)

    list.add(comp3)
    slist.add(simplesparse3)
    slist.add(simplesorted3)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4 = Array(sparse4, sorted4)
    val comp4 = new CompLongIntVector(dim * list4.length, list4)
    val simplesparse4 = VFactory.sparseLongKeyIntVector(dim * list1.length, simplelongrandIndices, intValues)
    val simplesorted4 = VFactory.sortedLongKeyIntVector(dim * list1.length, simplelongsortedIndices, intValues)

    longdummy = VFactory.longDummyVector(dim * 2, simplelongsortedIndices)

    list.add(comp4)
    slist.add(simplesparse4)
    slist.add(simplesorted4)
    slist.add(longdummy)
  }
}

class CompSimpleBinaryLongkeyOPTest {
  val list = CompSimpleBinaryLongkeyOPTest.list
  val slist = CompSimpleBinaryLongkeyOPTest.slist
  var longdummy = CompSimpleBinaryLongkeyOPTest.longdummy

  val times = 5
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def compAddsimpleTest() {
    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(slist.get(0))
      list.get(1).add(slist.get(2))
      list.get(2).add(slist.get(4))
      list.get(3).add(slist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse longkey add:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(slist.get(1))
      list.get(1).add(slist.get(3))
      list.get(2).add(slist.get(5))
      list.get(3).add(slist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted longkey add:$cost1")


    (0 until 1).foreach { i =>
      (i * 2 until 2).foreach { j =>
        if (getFlag(slist.get(j)) != "dummy") {
          assert(abs(list.get(i).add(slist.get(j)).sum() - (list.get(i).sum() + slist.get(j).sum())) < 1.0)
        } else {
          assert(abs(list.get(i).add(slist.get(j)).sum() - (list.get(i).sum() + longdummy.sum())) < 1.0)
        }
      }
    }
  }

  @Test
  def compSubsimpleTest() {
    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(slist.get(0))
      list.get(1).sub(slist.get(2))
      list.get(2).sub(slist.get(4))
      list.get(3).sub(slist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse longkey sub:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(slist.get(1))
      list.get(1).sub(slist.get(3))
      list.get(2).sub(slist.get(5))
      list.get(3).sub(slist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted longkey sub:$cost1")

    (0 until 1).foreach { i =>
      (i * 2 until 2).foreach { j =>
        if (getFlag(slist.get(j)) != "dummy") {
          assert(abs(list.get(i).sub(slist.get(j)).sum() - (list.get(i).sum() - slist.get(j).sum())) < 1.0)
        } else {
          assert(abs(list.get(i).sub(slist.get(j)).sum() - (list.get(i).sum() - longdummy.sum())) < 1.0)
        }
      }
    }
  }

  @Test
  def compMulsimpleTest() {
    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(slist.get(0))
      list.get(1).mul(slist.get(2))
      list.get(2).mul(slist.get(4))
      list.get(3).mul(slist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse longkey mul:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(slist.get(1))
      list.get(1).mul(slist.get(3))
      list.get(2).mul(slist.get(5))
      list.get(3).mul(slist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted longkey mul:$cost1")

    (0 until 1).foreach { i =>
      (i * 2 until 2).foreach { j =>
        println(s"${list.get(i).sum()}, ${slist.get(j).sum()}, ${list.get(i).mul(slist.get(j)).sum()}")
      }
    }
  }

  @Test
  def compDivsimpleTest() {
    (0 until 1).foreach { i =>
      (i * 2 until 2).foreach { j =>
        try{
          list.get(i).div(slist.get(j)).sum()
        }catch {
          case e:ArithmeticException =>{
            e
          }
          case e: AngelException => {
            e
          }
        }

      }
    }
  }

  @Test
  def compAxpysimpleTest() {
    //comp vs sparse
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(slist.get(0), 2.0)
      list.get(1).axpy(slist.get(2), 2.0)
      list.get(2).axpy(slist.get(4), 2.0)
      list.get(3).axpy(slist.get(6), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sparse longkey axpy:$cost1")

    //comp vs sorted
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(slist.get(1), 2.0)
      list.get(1).axpy(slist.get(3), 2.0)
      list.get(2).axpy(slist.get(5), 2.0)
      list.get(3).axpy(slist.get(7), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs sorted longkey axpy:$cost1")

    (0 until 1).foreach { i =>
      (i * 2 until 2).foreach { j =>
        assert(abs(list.get(i).axpy(slist.get(j), 2.0).sum() - (list.get(i).sum() + slist.get(j).sum() * 2)) < 1.0)
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