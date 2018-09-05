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
import com.tencent.angel.ml.math2.vector.{CompIntDoubleVector, CompIntFloatVector, CompIntIntVector, CompIntLongVector, CompLongDoubleVector, CompLongFloatVector, CompLongIntVector, CompLongLongVector, IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object CompSimpleCoverageTest {
  val capacity: Int = 1500
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

  val capacity1: Int = 10000
  val intrandIndices1: Array[Int] = new Array[Int](capacity1)
  val longrandIndices1: Array[Long] = new Array[Long](capacity1)
  val intsortedIndices1: Array[Int] = new Array[Int](capacity1)
  val longsortedIndices1: Array[Long] = new Array[Long](capacity1)

  val intValues1: Array[Int] = new Array[Int](capacity1)
  val longValues1: Array[Long] = new Array[Long](capacity1)
  val floatValues1: Array[Float] = new Array[Float](capacity1)
  val doubleValues1: Array[Double] = new Array[Double](capacity1)

  val intrandIndices2: Array[Int] = new Array[Int](capacity1)
  val intsortedIndices2: Array[Int] = new Array[Int](capacity1)
  val longrandIndices2: Array[Long] = new Array[Long](capacity1)
  val longsortedIndices2: Array[Long] = new Array[Long](capacity1)

  val simpledenseintValues: Array[Int] = new Array[Int](dim * 3)
  val simpledenselongValues: Array[Long] = new Array[Long](dim * 3)
  val simpledensefloatValues: Array[Float] = new Array[Float](dim * 3)
  val simpledensedoubleValues: Array[Double] = new Array[Double](dim * 3)

  val simpleintrandIndices: Array[Int] = new Array[Int](capacity)
  val simplelongrandIndices: Array[Long] = new Array[Long](capacity)
  val simpleintsortedIndices: Array[Int] = new Array[Int](capacity)
  val simplelongsortedIndices: Array[Long] = new Array[Long](capacity)

  val capacity2 = 180000
  val simpleintrandIndices1: Array[Int] = new Array[Int](capacity2)
  val simplelongrandIndices1: Array[Long] = new Array[Long](capacity2)
  val simpleintsortedIndices1: Array[Int] = new Array[Int](capacity2)
  val simplelongsortedIndices1: Array[Long] = new Array[Long](capacity2)

  val simpleintValues1: Array[Int] = new Array[Int](capacity2)
  val simplelongValues1: Array[Long] = new Array[Long](capacity2)
  val simplefloatValues1: Array[Float] = new Array[Float](capacity2)
  val simpledoubleValues1: Array[Double] = new Array[Double](capacity2)


  val ilist = new util.ArrayList[Vector]()
  val llist = new util.ArrayList[Vector]()
  val slist = new util.ArrayList[Vector]()
  val sllist = new util.ArrayList[Vector]()

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

    //other data
    set.clear()
    idx = 0
    while (set.size() < capacity1) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        intrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity1) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        longrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(intrandIndices1, 0, intsortedIndices1, 0, capacity1)
    util.Arrays.sort(intsortedIndices1)

    System.arraycopy(longrandIndices1, 0, longsortedIndices1, 0, capacity1)
    util.Arrays.sort(longsortedIndices1)

    doubleValues1.indices.foreach { i =>
      doubleValues1(i) = rand.nextDouble()
    }

    floatValues1.indices.foreach { i =>
      floatValues1(i) = rand.nextFloat()
    }

    longValues1.indices.foreach { i =>
      longValues1(i) = rand.nextInt(10) + 1L
    }

    intValues1.indices.foreach { i =>
      intValues1(i) = rand.nextInt(10) + 1
    }


    //other data2
    set.clear()
    idx = 0
    while (set.size() < capacity1) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        intrandIndices2(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity1) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        longrandIndices2(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(intrandIndices2, 0, intsortedIndices2, 0, capacity1)
    util.Arrays.sort(intsortedIndices2)

    System.arraycopy(longrandIndices2, 0, longsortedIndices2, 0, capacity1)
    util.Arrays.sort(longsortedIndices2)

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

    set.clear()
    idx = 0
    while (set.size() < capacity2) {
      val t = rand.nextInt(dim * 3)
      if (!set.contains(t)) {
        simpleintrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity2) {
      val t = rand.nextInt(dim * 2)
      if (!set.contains(t)) {
        simplelongrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(simpleintrandIndices1, 0, simpleintsortedIndices1, 0, capacity)
    util.Arrays.sort(simpleintsortedIndices)

    System.arraycopy(simplelongrandIndices1, 0, simplelongsortedIndices1, 0, capacity)
    util.Arrays.sort(simplelongsortedIndices1)

    simpledoubleValues1.indices.foreach { i =>
      simpledoubleValues1(i) = rand.nextDouble()
    }

    simplefloatValues1.indices.foreach { i =>
      simplefloatValues1(i) = rand.nextFloat()
    }

    simplelongValues1.indices.foreach { i =>
      simplelongValues1(i) = rand.nextInt(10) + 1L
    }

    simpleintValues1.indices.foreach { i =>
      simpleintValues1(i) = rand.nextInt(10) + 1
    }

    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = new CompIntDoubleVector(dim * list1.length, list1)
    val sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
    val sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
    val sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
    val list11 = Array(sparse11, sorted11, sorted12)
    val comp11 = new CompIntDoubleVector(dim * list11.length, list11)
    val list12 = Array(sparse1, sorted1, sparse1)
    val comp12 = new CompIntDoubleVector(dim * list12.length, list12)
    val simpledense1 = VFactory.denseDoubleVector(simpledensedoubleValues)
    val simplesparse1 = VFactory.sparseDoubleVector(dim * list1.length, simpleintrandIndices, doubleValues)
    val simplesorted1 = VFactory.sortedDoubleVector(dim * list1.length, simpleintsortedIndices, doubleValues)
    val simplesparse11 = VFactory.sparseDoubleVector(dim * list1.length, simpleintrandIndices1, simpledoubleValues1)
    val simplesorted11 = VFactory.sortedDoubleVector(dim * list1.length, simpleintsortedIndices1, simpledoubleValues1)
    val simplesorted12 = VFactory.sortedDoubleVector(dim * list1.length, simpleintsortedIndices1, simpledoubleValues1)

    ilist.add(comp1)
    ilist.add(comp11)
    ilist.add(comp12)
    slist.add(simpledense1)
    slist.add(simplesparse1)
    slist.add(simplesorted1)
    slist.add(simplesparse11)
    slist.add(simplesorted11)
    slist.add(simplesorted12)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = new CompIntFloatVector(dim * list2.length, list2)
    val sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
    val sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
    val sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
    val list21 = Array(sparse21, sorted21, sorted22)
    val comp21 = new CompIntFloatVector(dim * list21.length, list21)
    val list22 = Array(sparse2, sorted2, sparse2)
    val comp22 = new CompIntFloatVector(dim * list22.length, list22)
    val simpledense2 = VFactory.denseFloatVector(simpledensefloatValues)
    val simplesparse2 = VFactory.sparseFloatVector(dim * list2.length, simpleintrandIndices, floatValues)
    val simplesorted2 = VFactory.sortedFloatVector(dim * list2.length, simpleintsortedIndices, floatValues)
    val simplesparse21 = VFactory.sparseFloatVector(dim * list2.length, simpleintrandIndices1, simplefloatValues1)
    val simplesorted21 = VFactory.sortedFloatVector(dim * list2.length, simpleintsortedIndices1, simplefloatValues1)
    val simplesorted22 = VFactory.sortedFloatVector(dim * list2.length, simpleintsortedIndices1, simplefloatValues1)

    ilist.add(comp2)
    ilist.add(comp21)
    ilist.add(comp22)
    slist.add(simpledense2)
    slist.add(simplesparse2)
    slist.add(simplesorted2)
    slist.add(simplesparse21)
    slist.add(simplesorted21)
    slist.add(simplesorted22)


    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = new CompIntLongVector(dim * list3.length, list3)
    val sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
    val sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
    val sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
    val list31 = Array(sparse31, sorted31, sorted32)
    val comp31 = new CompIntLongVector(dim * list3.length, list31)
    val list32 = Array(sparse3, sorted3, sparse3)
    val comp32 = new CompIntLongVector(dim * list32.length, list32)
    val simpledense3 = VFactory.denseLongVector(simpledenselongValues)
    val simplesparse3 = VFactory.sparseLongVector(dim * list3.length, simpleintrandIndices, longValues)
    val simplesorted3 = VFactory.sortedLongVector(dim * list3.length, simpleintsortedIndices, longValues)
    val simplesparse31 = VFactory.sparseLongVector(dim * list3.length, simpleintrandIndices1, simplelongValues1)
    val simplesorted31 = VFactory.sortedLongVector(dim * list3.length, simpleintsortedIndices1, simplelongValues1)
    val simplesorted32 = VFactory.sortedLongVector(dim * list3.length, simpleintsortedIndices1, simplelongValues1)

    ilist.add(comp3)
    ilist.add(comp31)
    ilist.add(comp32)
    slist.add(simpledense3)
    slist.add(simplesparse3)
    slist.add(simplesorted3)
    slist.add(simplesparse31)
    slist.add(simplesorted31)
    slist.add(simplesorted32)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = new CompIntIntVector(dim * list4.length, list4)
    val sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
    val sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
    val sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
    val list41 = Array(sparse41, sorted41, sorted42)
    val comp41 = new CompIntIntVector(dim * list41.length, list41)
    val list42 = Array(sparse4, sorted4, sparse4)
    val comp42 = new CompIntIntVector(dim * list42.length, list42)
    val simpledense4 = VFactory.denseIntVector(simpledenseintValues)
    val simplesparse4 = VFactory.sparseIntVector(dim * list4.length, simpleintrandIndices, intValues)
    val simplesorted4 = VFactory.sortedIntVector(dim * list4.length, simpleintsortedIndices, intValues)
    val simplesparse41 = VFactory.sparseIntVector(dim * list4.length, simpleintrandIndices1, simpleintValues1)
    val simplesorted41 = VFactory.sortedIntVector(dim * list4.length, simpleintsortedIndices1, simpleintValues1)
    val simplesorted42 = VFactory.sortedIntVector(dim * list4.length, simpleintsortedIndices1, simpleintValues1)

    ilist.add(comp4)
    ilist.add(comp41)
    ilist.add(comp42)
    slist.add(simpledense4)
    slist.add(simplesparse4)
    slist.add(simplesorted4)
    slist.add(simplesparse41)
    slist.add(simplesorted41)
    slist.add(simplesorted42)


    val lsparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val lsorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val llist1 = Array(lsparse1, lsorted1)
    val lcomp1 = new CompLongDoubleVector(dim * llist1.length, llist1)
    val lsparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
    val lsorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
    val lsorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)
    val llist11 = Array(lsparse11, lsorted11)
    val llist12 = Array(lsparse11, lsorted12)
    val lcomp11 = new CompLongDoubleVector(dim * llist11.length, llist11)
    val lcomp12 = new CompLongDoubleVector(dim * llist12.length, llist12)
    val lsimplesparse1 = VFactory.sparseLongKeyDoubleVector(dim * llist1.length, simplelongrandIndices, doubleValues)
    val lsimplesorted1 = VFactory.sortedLongKeyDoubleVector(dim * llist1.length, simplelongsortedIndices, doubleValues)
    val lsimplesparse11 = VFactory.sparseLongKeyDoubleVector(dim * llist1.length, simplelongrandIndices1, simpledoubleValues1)
    val lsimplesorted11 = VFactory.sortedLongKeyDoubleVector(dim * llist1.length, simplelongsortedIndices1, simpledoubleValues1)
    val lsimplesorted12 = VFactory.sortedLongKeyDoubleVector(dim * llist1.length, simplelongsortedIndices1, simpledoubleValues1)

    llist.add(lcomp1)
    llist.add(lcomp11)
    llist.add(lcomp12)
    sllist.add(lsimplesparse1)
    sllist.add(lsimplesorted1)
    sllist.add(lsimplesparse11)
    sllist.add(lsimplesorted11)
    sllist.add(lsimplesorted12)


    val lsparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lsorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val llist2 = Array(lsparse2, lsorted2)
    val lcomp2 = new CompLongFloatVector(dim * llist2.length, llist2)
    val lsparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
    val lsorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
    val lsorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
    val llist21 = Array(lsparse21, lsorted21)
    val llist22 = Array(lsparse21, lsorted22)
    val lcomp21 = new CompLongFloatVector(dim * llist21.length, llist21)
    val lcomp22 = new CompLongFloatVector(dim * llist22.length, llist22)
    val lsimplesparse2 = VFactory.sparseLongKeyFloatVector(dim * llist2.length, simplelongrandIndices, floatValues)
    val lsimplesorted2 = VFactory.sortedLongKeyFloatVector(dim * llist2.length, simplelongsortedIndices, floatValues)
    val lsimplesparse21 = VFactory.sparseLongKeyFloatVector(dim * llist2.length, simplelongrandIndices1, simplefloatValues1)
    val lsimplesorted21 = VFactory.sortedLongKeyFloatVector(dim * llist2.length, simplelongsortedIndices1, simplefloatValues1)
    val lsimplesorted22 = VFactory.sortedLongKeyFloatVector(dim * llist2.length, simplelongsortedIndices1, simplefloatValues1)

    llist.add(lcomp2)
    llist.add(lcomp21)
    llist.add(lcomp22)
    sllist.add(lsimplesparse2)
    sllist.add(lsimplesorted2)
    sllist.add(lsimplesparse21)
    sllist.add(lsimplesorted21)
    sllist.add(lsimplesorted22)

    val lsparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val lsorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val llist3 = Array(lsparse3, lsorted3)
    val lcomp3 = new CompLongLongVector(dim * llist3.length, llist3)
    val lsparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
    val lsorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
    val lsorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
    val llist31 = Array(lsparse31, lsorted31)
    val llist32 = Array(lsparse31, lsorted32)
    val lcomp31 = new CompLongLongVector(dim * llist31.length, llist31)
    val lcomp32 = new CompLongLongVector(dim * llist32.length, llist32)
    val lsimplesparse3 = VFactory.sparseLongKeyLongVector(dim * llist3.length, simplelongrandIndices, longValues)
    val lsimplesorted3 = VFactory.sortedLongKeyLongVector(dim * llist3.length, simplelongsortedIndices, longValues)
    val lsimplesparse31 = VFactory.sparseLongKeyLongVector(dim * llist3.length, simplelongrandIndices1, simplelongValues1)
    val lsimplesorted31 = VFactory.sortedLongKeyLongVector(dim * llist3.length, simplelongsortedIndices1, simplelongValues1)
    val lsimplesorted32 = VFactory.sortedLongKeyLongVector(dim * llist3.length, simplelongsortedIndices1, simplelongValues1)

    llist.add(lcomp3)
    llist.add(lcomp31)
    llist.add(lcomp32)
    sllist.add(lsimplesparse3)
    sllist.add(lsimplesorted3)
    sllist.add(lsimplesparse31)
    sllist.add(lsimplesorted31)
    sllist.add(lsimplesorted32)

    val lsparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lsorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val llist4 = Array(lsparse4, lsorted4)
    val lcomp4 = new CompLongIntVector(dim * llist4.length, llist4)
    val lsparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
    val lsorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
    val lsorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
    val llist41 = Array(lsparse41, lsorted41)
    val llist42 = Array(lsparse41, lsorted42)
    val lcomp41 = new CompLongIntVector(dim * llist41.length, llist41)
    val lcomp42 = new CompLongIntVector(dim * llist42.length, llist42)
    val lsimplesparse4 = VFactory.sparseLongKeyIntVector(dim * llist4.length, simplelongrandIndices, intValues)
    val lsimplesorted4 = VFactory.sortedLongKeyIntVector(dim * llist4.length, simplelongsortedIndices, intValues)
    val lsimplesparse41 = VFactory.sparseLongKeyIntVector(dim * llist4.length, simplelongrandIndices1, simpleintValues1)
    val lsimplesorted41 = VFactory.sortedLongKeyIntVector(dim * llist4.length, simplelongsortedIndices1, simpleintValues1)
    val lsimplesorted42 = VFactory.sortedLongKeyIntVector(dim * llist4.length, simplelongsortedIndices1, simpleintValues1)

    llist.add(lcomp4)
    llist.add(lcomp41)
    llist.add(lcomp42)
    sllist.add(lsimplesparse4)
    sllist.add(lsimplesorted4)
    sllist.add(lsimplesparse41)
    sllist.add(lsimplesorted41)
    sllist.add(lsimplesorted42)
  }
}

class CompSimpleCoverageTest {
  val capacity: Int = CompSimpleCoverageTest.capacity
  val dim: Int = CompSimpleCoverageTest.dim

  val intrandIndices: Array[Int] = CompSimpleCoverageTest.intrandIndices
  val longrandIndices: Array[Long] = CompSimpleCoverageTest.longrandIndices
  val intsortedIndices: Array[Int] = CompSimpleCoverageTest.intsortedIndices
  val longsortedIndices: Array[Long] = CompSimpleCoverageTest.longsortedIndices

  val intValues: Array[Int] = CompSimpleCoverageTest.intValues
  val longValues: Array[Long] = CompSimpleCoverageTest.longValues
  val floatValues: Array[Float] = CompSimpleCoverageTest.floatValues
  val doubleValues: Array[Double] = CompSimpleCoverageTest.doubleValues

  val denseintValues: Array[Int] = CompSimpleCoverageTest.denseintValues
  val denselongValues: Array[Long] = CompSimpleCoverageTest.denselongValues
  val densefloatValues: Array[Float] = CompSimpleCoverageTest.densefloatValues
  val densedoubleValues: Array[Double] = CompSimpleCoverageTest.densedoubleValues

  val capacity1: Int = CompSimpleCoverageTest.capacity1
  val intrandIndices1: Array[Int] = CompSimpleCoverageTest.intrandIndices1
  val longrandIndices1: Array[Long] = CompSimpleCoverageTest.longrandIndices1
  val intsortedIndices1: Array[Int] = CompSimpleCoverageTest.intsortedIndices1
  val longsortedIndices1: Array[Long] = CompSimpleCoverageTest.longsortedIndices1

  val intValues1: Array[Int] = CompSimpleCoverageTest.intValues1
  val longValues1: Array[Long] = CompSimpleCoverageTest.longValues1
  val floatValues1: Array[Float] = CompSimpleCoverageTest.floatValues1
  val doubleValues1: Array[Double] = CompSimpleCoverageTest.doubleValues1

  val intrandIndices2: Array[Int] = CompSimpleCoverageTest.intrandIndices2
  val intsortedIndices2: Array[Int] = CompSimpleCoverageTest.intsortedIndices2
  val longrandIndices2: Array[Long] = CompSimpleCoverageTest.longrandIndices2
  val longsortedIndices2: Array[Long] = CompSimpleCoverageTest.longsortedIndices2


  val simpleintrandIndices: Array[Int] = CompSimpleCoverageTest.simpleintrandIndices
  val simplelongrandIndices: Array[Long] = CompSimpleCoverageTest.simplelongrandIndices
  val simpleintsortedIndices: Array[Int] = CompSimpleCoverageTest.simpleintsortedIndices
  val simplelongsortedIndices: Array[Long] = CompSimpleCoverageTest.simplelongsortedIndices

  val capacity2 = CompSimpleCoverageTest.capacity2
  val simpleintrandIndices1: Array[Int] = CompSimpleCoverageTest.simpleintrandIndices1
  val simplelongrandIndices1: Array[Long] = CompSimpleCoverageTest.simplelongrandIndices1
  val simpleintsortedIndices1: Array[Int] = CompSimpleCoverageTest.simpleintsortedIndices1
  val simplelongsortedIndices1: Array[Long] = CompSimpleCoverageTest.simplelongsortedIndices1


  val ilist = CompSimpleCoverageTest.ilist
  val llist = CompSimpleCoverageTest.llist
  val slist = CompSimpleCoverageTest.slist
  val sllist = CompSimpleCoverageTest.sllist

  @Test
  def CompAddsimpleTest() {
    (0 until ilist.size()).foreach { i =>
      ((i / 3) * 6 until slist.size()).foreach { j =>
       (ilist.get(i).add(slist.get(j))).sum()
      }
    }

    //longkey
    (0 until llist.size()).foreach { i =>
      ((i / 3) * 6 until sllist.size()).foreach { j =>
       (llist.get(i).add(sllist.get(j))).sum()
      }
    }
  }

  @Test
  def CompSubsimpleTest() {
    (0 until ilist.size()).foreach { i =>
      ((i / 3) * 6 until slist.size()).foreach { j =>
        (ilist.get(i).sub(slist.get(j))).sum()
      }
    }

    //longkey
    (0 until llist.size()).foreach { i =>
      ((i / 3) * 6 until sllist.size()).foreach { j =>
       (llist.get(i).sub(sllist.get(j))).sum()
      }
    }
  }

  @Test
  def CompMulsimpleTest() {
    (0 until ilist.size()).foreach { i =>
      ((i / 3) * 6 until slist.size()).foreach { j =>
        (ilist.get(i).mul(slist.get(j))).sum()
      }
    }

    //longkey
    (0 until llist.size()).foreach { i =>
      ((i / 3) * 6 until sllist.size()).foreach { j =>
        (llist.get(i).mul(sllist.get(j))).sum()
      }
    }
  }

  @Test
  def CompDivsimpleTest() {
    (0 until ilist.size()).foreach { i =>
      ((i / 3) * 6 until slist.size()).foreach { j =>
        try{
          (ilist.get(i).div(slist.get(j))).sum()
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

    //longkey
    (0 until llist.size()).foreach { i =>
      ((i / 3) * 6 until sllist.size()).foreach { j =>
        try{
          (llist.get(i).div(sllist.get(j))).sum()
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
  def CompAxpysimpleTest() {
    (0 until ilist.size()).foreach { i =>
      ((i / 3) * 6 until slist.size()).foreach { j =>
        (ilist.get(i).axpy(slist.get(j), 2.0)).sum()
      }
    }

    //longkey
    (0 until llist.size()).foreach { i =>
      ((i / 3) * 6 until sllist.size()).foreach { j =>
        (llist.get(i).axpy(sllist.get(j), 2.0)).sum()
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
