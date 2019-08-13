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


package com.tencent.angel.ml.math2.matrix.RBMatrixTest

import java.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.MathException
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object RBCompMatrixTest {
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

  val matrixlist = new util.ArrayList[Matrix]()
  val vectorlist = new util.ArrayList[Vector]()

  val lmatrixlist = new util.ArrayList[Matrix]()
  val lvectorlist = new util.ArrayList[Vector]()

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
    val comp1_1 = VFactory.compIntDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = VFactory.compIntDoubleVector(dim * list1_2.length, list1_2)
    val comp1_3 = VFactory.compIntDoubleVector(dim * list1_3.length, list1_3)
    val comp1_4 = VFactory.compIntDoubleVector(dim * list1_4.length, list1_4)
    val comp1_5 = VFactory.compIntDoubleVector(dim * list1_5.length, list1_5)
    val comp1_6 = VFactory.compIntDoubleVector(dim * list1_6.length, list1_6)
    val sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
    val sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
    val sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
    val list11 = Array(sparse11, sorted11, sorted12)
    val comp11 = VFactory.compIntDoubleVector(dim * list11.length, list11)
    val list12 = Array(sorted11, sorted12, sparse11)
    val comp12 = VFactory.compIntDoubleVector(dim * list12.length, list12)

    vectorlist.add(comp1_1)
    vectorlist.add(comp1_2)
    vectorlist.add(comp1_3)
    vectorlist.add(comp1_4)
    vectorlist.add(comp1_5)
    vectorlist.add(comp1_6)
    vectorlist.add(comp11)
    vectorlist.add(comp12)

    matrixlist.add(MFactory.rbCompIntDoubleMatrix(Array(comp1_1, comp1_2, comp1_3, comp1_4, comp1_5, comp1_6)))

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2_1 = Array(dense2, sparse2, sorted2)
    val list2_2 = Array(dense2, sorted2, sparse2)
    val list2_3 = Array(sparse2, dense2, sorted2)
    val list2_4 = Array(sparse2, sorted2, dense2)
    val list2_5 = Array(sorted2, dense2, sparse2)
    val list2_6 = Array(sorted2, sparse2, dense2)
    val comp2_1 = VFactory.compIntFloatVector(dim * list2_1.length, list2_1)
    val comp2_2 = VFactory.compIntFloatVector(dim * list2_2.length, list2_2)
    val comp2_3 = VFactory.compIntFloatVector(dim * list2_3.length, list2_3)
    val comp2_4 = VFactory.compIntFloatVector(dim * list2_4.length, list2_4)
    val comp2_5 = VFactory.compIntFloatVector(dim * list2_5.length, list2_5)
    val comp2_6 = VFactory.compIntFloatVector(dim * list2_6.length, list2_6)
    val sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
    val sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
    val sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
    val list21 = Array(sparse21, sorted21, sorted22)
    val comp21 = VFactory.compIntFloatVector(dim * list21.length, list21)
    val list22 = Array(sorted21, sorted22, sparse21)
    val comp22 = VFactory.compIntFloatVector(dim * list22.length, list22)

    vectorlist.add(comp2_1)
    vectorlist.add(comp2_2)
    vectorlist.add(comp2_3)
    vectorlist.add(comp2_4)
    vectorlist.add(comp2_5)
    vectorlist.add(comp2_6)
    vectorlist.add(comp21)
    vectorlist.add(comp22)

    matrixlist.add(MFactory.rbCompIntFloatMatrix(Array(comp2_1, comp2_2, comp2_3, comp2_4, comp2_5, comp2_6, comp21, comp22)))


    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3_1 = Array(dense3, sparse3, sorted3)
    val list3_2 = Array(dense3, sorted3, sparse3)
    val list3_3 = Array(sparse3, dense3, sorted3)
    val list3_4 = Array(sparse3, sorted3, dense3)
    val list3_5 = Array(sorted3, dense3, sparse3)
    val list3_6 = Array(sorted3, sparse3, dense3)
    val comp3_1 = VFactory.compIntLongVector(dim * list3_1.length, list3_1)
    val comp3_2 = VFactory.compIntLongVector(dim * list3_2.length, list3_2)
    val comp3_3 = VFactory.compIntLongVector(dim * list3_3.length, list3_3)
    val comp3_4 = VFactory.compIntLongVector(dim * list3_4.length, list3_4)
    val comp3_5 = VFactory.compIntLongVector(dim * list3_5.length, list3_5)
    val comp3_6 = VFactory.compIntLongVector(dim * list3_6.length, list3_6)
    val sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
    val sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
    val sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
    val list31 = Array(sparse31, sorted31, sorted32)
    val comp31 = VFactory.compIntLongVector(dim * list31.length, list31)
    val list32 = Array(sorted31, sorted32, sparse31)
    val comp32 = VFactory.compIntLongVector(dim * list32.length, list32)

    vectorlist.add(comp3_1)
    vectorlist.add(comp3_2)
    vectorlist.add(comp3_3)
    vectorlist.add(comp3_4)
    vectorlist.add(comp3_5)
    vectorlist.add(comp3_6)
    vectorlist.add(comp31)
    vectorlist.add(comp32)

    matrixlist.add(MFactory.rbCompIntLongMatrix(Array(comp3_1, comp3_2, comp3_3, comp3_4, comp3_5, comp3_6, comp31, comp32)))


    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4_1 = Array(dense4, sparse4, sorted4)
    val list4_2 = Array(dense4, sorted4, sparse4)
    val list4_3 = Array(sparse4, dense4, sorted4)
    val list4_4 = Array(sparse4, sorted4, dense4)
    val list4_5 = Array(sorted4, dense4, sparse4)
    val list4_6 = Array(sorted4, sparse4, dense4)
    val comp4_1 = VFactory.compIntIntVector(dim * list4_1.length, list4_1)
    val comp4_2 = VFactory.compIntIntVector(dim * list4_2.length, list4_2)
    val comp4_3 = VFactory.compIntIntVector(dim * list4_3.length, list4_3)
    val comp4_4 = VFactory.compIntIntVector(dim * list4_4.length, list4_4)
    val comp4_5 = VFactory.compIntIntVector(dim * list4_5.length, list4_5)
    val comp4_6 = VFactory.compIntIntVector(dim * list4_6.length, list4_6)
    val sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
    val sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
    val sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
    val list41 = Array(sparse41, sorted41, sorted42)
    val comp41 = VFactory.compIntIntVector(dim * list41.length, list41)
    val list42 = Array(sorted41, sorted42, sparse41)
    val comp42 = VFactory.compIntIntVector(dim * list42.length, list42)

    vectorlist.add(comp4_1)
    vectorlist.add(comp4_2)
    vectorlist.add(comp4_3)
    vectorlist.add(comp4_4)
    vectorlist.add(comp4_5)
    vectorlist.add(comp4_6)
    vectorlist.add(comp41)
    vectorlist.add(comp42)

    matrixlist.add(MFactory.rbCompIntIntMatrix(Array(comp4_1, comp4_2, comp4_3, comp4_4, comp4_5, comp4_6, comp41, comp42)))


    val lsparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val lsorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val llist1_1 = Array(lsparse1, lsorted1)
    val llist1_2 = Array(lsorted1, lsparse1)
    val lcomp1_1 = VFactory.compLongDoubleVector(dim * llist1_1.length, llist1_1)
    val lcomp1_2 = VFactory.compLongDoubleVector(dim * llist1_2.length, llist1_2)
    val lsparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
    val lsorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
    val lsorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)
    val llist11 = Array(lsparse11, lsorted11)
    val llist12 = Array(lsorted12, lsparse11)
    val lcomp11 = VFactory.compLongDoubleVector(dim * llist11.length, llist11)
    val lcomp12 = VFactory.compLongDoubleVector(dim * llist12.length, llist12)

    lvectorlist.add(lcomp1_1)
    lvectorlist.add(lcomp1_2)
    lvectorlist.add(lcomp11)
    lvectorlist.add(lcomp12)
    lmatrixlist.add(MFactory.rbCompLongDoubleMatrix(Array(lcomp1_1, lcomp1_2, lcomp11, lcomp12)))


    val lsparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lsorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val llist2_1 = Array(lsparse2, lsorted2)
    val llist2_2 = Array(lsorted2, lsparse2)
    val lcomp2_1 = VFactory.compLongFloatVector(dim * llist2_1.length, llist2_1)
    val lcomp2_2 = VFactory.compLongFloatVector(dim * llist2_2.length, llist2_2)
    val lsparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
    val lsorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
    val lsorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
    val llist21 = Array(lsparse21, lsorted21)
    val llist22 = Array(lsorted22, lsparse21)
    val lcomp21 = VFactory.compLongFloatVector(dim * llist21.length, llist21)
    val lcomp22 = VFactory.compLongFloatVector(dim * llist22.length, llist22)

    lvectorlist.add(lcomp2_1)
    lvectorlist.add(lcomp2_2)
    lvectorlist.add(lcomp21)
    lvectorlist.add(lcomp22)
    lmatrixlist.add(MFactory.rbCompLongFloatMatrix(Array(lcomp2_1, lcomp2_2, lcomp21, lcomp22)))

    val lsparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val lsorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val llist3_1 = Array(lsparse3, lsorted3)
    val llist3_2 = Array(lsorted3, lsparse3)
    val lcomp3_1 = VFactory.compLongLongVector(dim * llist3_1.length, llist3_1)
    val lcomp3_2 = VFactory.compLongLongVector(dim * llist3_2.length, llist3_2)
    val lsparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
    val lsorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
    val lsorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
    val llist31 = Array(lsparse31, lsorted31)
    val llist32 = Array(lsorted32, lsparse31)
    val lcomp31 = VFactory.compLongLongVector(dim * llist31.length, llist31)
    val lcomp32 = VFactory.compLongLongVector(dim * llist32.length, llist32)

    lvectorlist.add(lcomp3_1)
    lvectorlist.add(lcomp3_2)
    lvectorlist.add(lcomp31)
    lvectorlist.add(lcomp32)
    lmatrixlist.add(MFactory.rbCompLongLongMatrix(Array(lcomp3_1, lcomp3_2, lcomp31, lcomp32)))

    val lsparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lsorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val llist4_1 = Array(lsparse4, lsorted4)
    val llist4_2 = Array(lsorted4, lsparse4)
    val lcomp4_1 = VFactory.compLongIntVector(dim * llist4_1.length, llist4_1)
    val lcomp4_2 = VFactory.compLongIntVector(dim * llist4_2.length, llist4_2)
    val lsparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
    val lsorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
    val lsorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
    val llist41 = Array(lsparse41, lsorted41)
    val llist42 = Array(lsorted42, lsparse41)
    val lcomp41 = VFactory.compLongIntVector(dim * llist41.length, llist41)
    val lcomp42 = VFactory.compLongIntVector(dim * llist42.length, llist42)

    lvectorlist.add(lcomp4_1)
    lvectorlist.add(lcomp4_2)
    lvectorlist.add(lcomp41)
    lvectorlist.add(lcomp42)
    lmatrixlist.add(MFactory.rbCompLongIntMatrix(Array(lcomp4_1, lcomp4_2, lcomp41, lcomp42)))
  }
}

class RBCompMatrixTest {
  val matrixlist = RBCompMatrixTest.matrixlist
  val vectorlist = RBCompMatrixTest.vectorlist

  val lmatrixlist = RBCompMatrixTest.lmatrixlist
  val lvectorlist = RBCompMatrixTest.lvectorlist


  @Test
  def addTest() {
    println(matrixlist.get(0).sum())
    println(matrixlist.get(0).add(2).sum())
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).add(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
         matrixlist.get(i).add(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
     matrixlist.get(i).add(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).add(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).add(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def subTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
         matrixlist.get(i).sub(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).sub(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      matrixlist.get(i).sub(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).sub(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).sub(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
    }
  }

  @Test
  def mulTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).mul(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).mul(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      matrixlist.get(i).mul(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).mul(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).mul(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
        }
      }
     lmatrixlist.get(i).mul(2).sum()
    }
  }

  @Test
  def divTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).div(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).div(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
     matrixlist.get(i).div(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).div(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).div(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
      lmatrixlist.get(i).div(2).sum()
    }
  }

  @Test
  def axpyTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
         matrixlist.get(i).axpy(matrixlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
         matrixlist.get(i).axpy(vectorlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).axpy(lmatrixlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).axpy(lvectorlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => e
          case e: MathException => e
          case e: ArithmeticException => e
        }
      }
    }
  }


  @Test
  def reduceTest() {
    (0 until matrixlist.size).foreach { i =>
     matrixlist.get(i).norm()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      lmatrixlist.get(i).norm()
    }
  }

  @Test
  def diagTest() {
    (0 until matrixlist.size).foreach { i =>
      matrixlist.get(i).diag().sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      lmatrixlist.get(i).diag().sum()
    }
  }
}
