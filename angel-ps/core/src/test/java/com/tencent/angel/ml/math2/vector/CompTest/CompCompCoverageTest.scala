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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.utils.MathException
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompCompCoverageTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
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

  val capacity1: Int = 80000
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

  val list = new util.ArrayList[Vector]()
  val llist = new util.ArrayList[Vector]()

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

    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1_1 = Array(dense1, sparse1, sorted1)
    val list1_2 = Array(dense1, sorted1, sparse1)
    val list1_3 = Array(sparse1, dense1, sorted1)
    val list1_4 = Array(sparse1, sorted1, dense1)
    val list1_5 = Array(sorted1, dense1, sparse1)
    val list1_6 = Array(sorted1, sparse1, dense1)
    val list1_7 = Array(dense1, dense1, dense1)
    val list1_8 = Array(sparse1, sparse1, sparse1)
    val list1_9 = Array(sorted1, sorted1, sorted1)
    val comp1_1 = new CompIntDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = new CompIntDoubleVector(dim * list1_2.length, list1_2)
    val comp1_3 = new CompIntDoubleVector(dim * list1_3.length, list1_3)
    val comp1_4 = new CompIntDoubleVector(dim * list1_4.length, list1_4)
    val comp1_5 = new CompIntDoubleVector(dim * list1_5.length, list1_5)
    val comp1_6 = new CompIntDoubleVector(dim * list1_6.length, list1_6)
    val comp1_7 = new CompIntDoubleVector(dim * list1_7.length, list1_7)
    val comp1_8 = new CompIntDoubleVector(dim * list1_8.length, list1_8)
    val comp1_9 = new CompIntDoubleVector(dim * list1_9.length, list1_9)

    val sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
    val sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
    val sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
    val list11 = Array(sparse11, sorted11, sorted12)
    val comp11 = new CompIntDoubleVector(dim * list11.length, list11)
    val list12 = Array(sorted11, sorted12, sparse11)
    val comp12 = new CompIntDoubleVector(dim * list12.length, list12)

    list.add(comp1_1)
    list.add(comp1_2)
    list.add(comp1_3)
    list.add(comp1_4)
    list.add(comp1_5)
    list.add(comp1_6)
    list.add(comp1_7)
    list.add(comp1_8)
    list.add(comp1_9)
    list.add(comp11)
    list.add(comp12)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2_1 = Array(dense2, sparse2, sorted2)
    val list2_2 = Array(dense2, sorted2, sparse2)
    val list2_3 = Array(sparse2, dense2, sorted2)
    val list2_4 = Array(sparse2, sorted2, dense2)
    val list2_5 = Array(sorted2, dense2, sparse2)
    val list2_6 = Array(sorted2, sparse2, dense2)
    val list2_7 = Array(dense2, dense2, dense2)
    val list2_8 = Array(sparse2, sparse2, sparse2)
    val list2_9 = Array(sorted2, sorted2, sorted2)
    val comp2_1 = new CompIntFloatVector(dim * list2_1.length, list2_1)
    val comp2_2 = new CompIntFloatVector(dim * list2_2.length, list2_2)
    val comp2_3 = new CompIntFloatVector(dim * list2_3.length, list2_3)
    val comp2_4 = new CompIntFloatVector(dim * list2_4.length, list2_4)
    val comp2_5 = new CompIntFloatVector(dim * list2_5.length, list2_5)
    val comp2_6 = new CompIntFloatVector(dim * list2_6.length, list2_6)
    val comp2_7 = new CompIntFloatVector(dim * list2_7.length, list2_7)
    val comp2_8 = new CompIntFloatVector(dim * list2_8.length, list2_8)
    val comp2_9 = new CompIntFloatVector(dim * list2_9.length, list2_9)
    val sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
    val sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
    val sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
    val list21 = Array(sparse21, sorted21, sorted22)
    val comp21 = new CompIntFloatVector(dim * list21.length, list21)
    val list22 = Array(sorted21, sorted22, sparse21)
    val comp22 = new CompIntFloatVector(dim * list22.length, list22)

    list.add(comp2_1)
    list.add(comp2_2)
    list.add(comp2_3)
    list.add(comp2_4)
    list.add(comp2_5)
    list.add(comp2_6)
    list.add(comp2_7)
    list.add(comp2_8)
    list.add(comp2_9)
    list.add(comp21)
    list.add(comp22)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3_1 = Array(dense3, sparse3, sorted3)
    val list3_2 = Array(dense3, sorted3, sparse3)
    val list3_3 = Array(sparse3, dense3, sorted3)
    val list3_4 = Array(sparse3, sorted3, dense3)
    val list3_5 = Array(sorted3, dense3, sparse3)
    val list3_6 = Array(sorted3, sparse3, dense3)
    val comp3_1 = new CompIntLongVector(dim * list3_1.length, list3_1)
    val comp3_2 = new CompIntLongVector(dim * list3_2.length, list3_2)
    val comp3_3 = new CompIntLongVector(dim * list3_3.length, list3_3)
    val comp3_4 = new CompIntLongVector(dim * list3_4.length, list3_4)
    val comp3_5 = new CompIntLongVector(dim * list3_5.length, list3_5)
    val comp3_6 = new CompIntLongVector(dim * list3_6.length, list3_6)
    val sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
    val sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
    val sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
    val list31 = Array(sparse31, sorted31, sorted32)
    val comp31 = new CompIntLongVector(dim * list31.length, list31)
    val list32 = Array(sorted31, sorted32, sparse31)
    val comp32 = new CompIntLongVector(dim * list32.length, list32)

    list.add(comp3_1)
    list.add(comp3_2)
    list.add(comp3_3)
    list.add(comp3_4)
    list.add(comp3_5)
    list.add(comp3_6)
    list.add(comp31)
    list.add(comp32)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4_1 = Array(dense4, sparse4, sorted4)
    val list4_2 = Array(dense4, sorted4, sparse4)
    val list4_3 = Array(sparse4, dense4, sorted4)
    val list4_4 = Array(sparse4, sorted4, dense4)
    val list4_5 = Array(sorted4, dense4, sparse4)
    val list4_6 = Array(sorted4, sparse4, dense4)
    val comp4_1 = new CompIntIntVector(dim * list4_1.length, list4_1)
    val comp4_2 = new CompIntIntVector(dim * list4_2.length, list4_2)
    val comp4_3 = new CompIntIntVector(dim * list4_3.length, list4_3)
    val comp4_4 = new CompIntIntVector(dim * list4_4.length, list4_4)
    val comp4_5 = new CompIntIntVector(dim * list4_5.length, list4_5)
    val comp4_6 = new CompIntIntVector(dim * list4_6.length, list4_6)
    val sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
    val sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
    val sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
    val list41 = Array(sparse41, sorted41, sorted42)
    val comp41 = new CompIntIntVector(dim * list41.length, list41)
    val list42 = Array(sorted41, sorted42, sparse41)
    val comp42 = new CompIntIntVector(dim * list42.length, list42)

    list.add(comp4_1)
    list.add(comp4_2)
    list.add(comp4_3)
    list.add(comp4_4)
    list.add(comp4_5)
    list.add(comp4_6)
    list.add(comp41)
    list.add(comp42)

    val lsparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val lsorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val llist1_1 = Array(lsparse1, lsorted1)
    val llist1_2 = Array(lsorted1, lsparse1)
    val lcomp1_1 = new CompLongDoubleVector(dim * llist1_1.length, llist1_1)
    val lcomp1_2 = new CompLongDoubleVector(dim * llist1_2.length, llist1_2)
    val lsparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
    val lsorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
    val lsorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)
    val llist11 = Array(lsparse11, lsorted11)
    val llist12 = Array(lsorted12, lsparse11)
    val lcomp11 = new CompLongDoubleVector(dim * llist11.length, llist11)
    val lcomp12 = new CompLongDoubleVector(dim * llist12.length, llist12)

    llist.add(lcomp1_1)
    llist.add(lcomp1_2)
    llist.add(lcomp11)
    llist.add(lcomp12)

    val lsparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lsorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val llist2_1 = Array(lsparse2, lsorted2)
    val llist2_2 = Array(lsorted2, lsparse2)
    val lcomp2_1 = new CompLongFloatVector(dim * llist2_1.length, llist2_1)
    val lcomp2_2 = new CompLongFloatVector(dim * llist2_2.length, llist2_2)
    val lsparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
    val lsorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
    val lsorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
    val llist21 = Array(lsparse21, lsorted21)
    val llist22 = Array(lsorted22, lsparse21)
    val lcomp21 = new CompLongFloatVector(dim * llist21.length, llist21)
    val lcomp22 = new CompLongFloatVector(dim * llist22.length, llist22)

    llist.add(lcomp2_1)
    llist.add(lcomp2_2)
    llist.add(lcomp21)
    llist.add(lcomp22)

    val lsparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val lsorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val llist3_1 = Array(lsparse3, lsorted3)
    val llist3_2 = Array(lsorted3, lsparse3)
    val lcomp3_1 = new CompLongLongVector(dim * llist3_1.length, llist3_1)
    val lcomp3_2 = new CompLongLongVector(dim * llist3_2.length, llist3_2)
    val lsparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
    val lsorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
    val lsorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
    val llist31 = Array(lsparse31, lsorted31)
    val llist32 = Array(lsorted32, lsparse31)
    val lcomp31 = new CompLongLongVector(dim * llist31.length, llist31)
    val lcomp32 = new CompLongLongVector(dim * llist32.length, llist32)

    llist.add(lcomp3_1)
    llist.add(lcomp3_2)
    llist.add(lcomp31)
    llist.add(lcomp32)

    val lsparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lsorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val llist4_1 = Array(lsparse4, lsorted4)
    val llist4_2 = Array(lsorted4, lsparse4)
    val lcomp4_1 = new CompLongIntVector(dim * llist4_1.length, llist4_1)
    val lcomp4_2 = new CompLongIntVector(dim * llist4_2.length, llist4_2)
    val lsparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
    val lsorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
    val lsorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
    val llist41 = Array(lsparse41, lsorted41)
    val llist42 = Array(lsorted42, lsparse41)
    val lcomp41 = new CompLongIntVector(dim * llist41.length, llist41)
    val lcomp42 = new CompLongIntVector(dim * llist42.length, llist42)

    llist.add(lcomp4_1)
    llist.add(lcomp4_2)
    llist.add(lcomp41)
    llist.add(lcomp42)
  }
}

class CompCompCoverageTest {
  val capacity: Int = CompCompCoverageTest.capacity
  val dim: Int = CompCompCoverageTest.dim

  val intrandIndices: Array[Int] = CompCompCoverageTest.intrandIndices
  val longrandIndices: Array[Long] = CompCompCoverageTest.longrandIndices
  val intsortedIndices: Array[Int] = CompCompCoverageTest.intsortedIndices
  val longsortedIndices: Array[Long] = CompCompCoverageTest.longsortedIndices

  val intValues: Array[Int] = CompCompCoverageTest.intValues
  val longValues: Array[Long] = CompCompCoverageTest.longValues
  val floatValues: Array[Float] = CompCompCoverageTest.floatValues
  val doubleValues: Array[Double] = CompCompCoverageTest.doubleValues

  val denseintValues: Array[Int] = CompCompCoverageTest.denseintValues
  val denselongValues: Array[Long] = CompCompCoverageTest.denselongValues
  val densefloatValues: Array[Float] = CompCompCoverageTest.densefloatValues
  val densedoubleValues: Array[Double] = CompCompCoverageTest.densedoubleValues

  val capacity1: Int = CompCompCoverageTest.capacity1
  val intrandIndices1: Array[Int] = CompCompCoverageTest.intrandIndices1
  val longrandIndices1: Array[Long] = CompCompCoverageTest.longrandIndices1
  val intsortedIndices1: Array[Int] = CompCompCoverageTest.intsortedIndices1
  val longsortedIndices1: Array[Long] = CompCompCoverageTest.longsortedIndices1

  val intValues1: Array[Int] = CompCompCoverageTest.intValues1
  val longValues1: Array[Long] = CompCompCoverageTest.longValues1
  val floatValues1: Array[Float] = CompCompCoverageTest.floatValues1
  val doubleValues1: Array[Double] = CompCompCoverageTest.doubleValues1

  val intrandIndices2: Array[Int] = CompCompCoverageTest.intrandIndices2
  val intsortedIndices2: Array[Int] = CompCompCoverageTest.intsortedIndices2
  val longrandIndices2: Array[Long] = CompCompCoverageTest.longrandIndices2
  val longsortedIndices2: Array[Long] = CompCompCoverageTest.longsortedIndices2

  val list = CompCompCoverageTest.list
  val llist = CompCompCoverageTest.llist

  @Test
  def compVScompAddTest() {
    (0 until list.size()).foreach { i =>
      (0 until list.size()).foreach { j =>
        try {
          list.get(i).add(list.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).add(llist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def compVScompSubTest() {
    (0 until list.size()).foreach { i =>
      (0 until list.size()).foreach { j =>
        try {
         list.get(i).mul(list.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).mul(llist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def compVScompMulTest() {
    (0 until list.size()).foreach { i =>
      (0 until list.size()).foreach { j =>
        try {
          list.get(i).mul(list.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).mul(llist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def compVScompDivTest() {
    (0 until list.size()).foreach { i =>
      (0 until list.size()).foreach { j =>
        try {
         list.get(i).div(list.get(j)).sum()
        } catch {
          case e:ArithmeticException => e
          case e: MathException => e
          case e: AngelException => e
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          llist.get(i).div(llist.get(j)).sum()
        } catch {
          case e:ArithmeticException => e
          case e: MathException => e
          case e: AngelException => e
        }
      }
    }
  }

  @Test
  def compVScompAxpyTest() {
    (0 until list.size()).foreach { i =>
      (0 until list.size()).foreach { j =>
        try {
          list.get(i).axpy(list.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).axpy(llist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

}
