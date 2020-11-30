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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

//1500,50000;1500,80000
object BinaryCoverageTest {
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


  val intValues_inplace: Array[Int] = new Array[Int](capacity)
  val longValues_inplace: Array[Long] = new Array[Long](capacity)
  val floatValues_inplace: Array[Float] = new Array[Float](capacity)
  val doubleValues_inplace: Array[Double] = new Array[Double](capacity)

  val denseintValues_inplace: Array[Int] = new Array[Int](dim)
  val denselongValues_inplace: Array[Long] = new Array[Long](dim)
  val densefloatValues_inplace: Array[Float] = new Array[Float](dim)
  val densedoubleValues_inplace: Array[Double] = new Array[Double](dim)

  val intValues1_inplace: Array[Int] = new Array[Int](capacity1)
  val longValues1_inplace: Array[Long] = new Array[Long](capacity1)
  val floatValues1_inplace: Array[Float] = new Array[Float](capacity1)
  val doubleValues1_inplace: Array[Double] = new Array[Double](capacity1)


  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val ilist = new util.ArrayList[Vector]()
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

    //data for inplace
    doubleValues_inplace.indices.foreach { i =>
      doubleValues_inplace(i) = rand.nextDouble()
    }

    floatValues_inplace.indices.foreach { i =>
      floatValues_inplace(i) = rand.nextFloat()
    }

    longValues_inplace.indices.foreach { i =>
      longValues_inplace(i) = rand.nextInt(10) + 1L
    }

    intValues_inplace.indices.foreach { i =>
      intValues_inplace(i) = rand.nextInt(10) + 1
    }


    densedoubleValues_inplace.indices.foreach { i =>
      densedoubleValues_inplace(i) = rand.nextDouble()
    }

    densefloatValues_inplace.indices.foreach { i =>
      densefloatValues_inplace(i) = rand.nextFloat()
    }

    denselongValues_inplace.indices.foreach { i =>
      denselongValues_inplace(i) = rand.nextInt(10) + 1L
    }

    denseintValues_inplace.indices.foreach { i =>
      denseintValues_inplace(i) = rand.nextInt(10) + 1
    }


    doubleValues1_inplace.indices.foreach { i =>
      doubleValues1_inplace(i) = rand.nextDouble()
    }

    floatValues1_inplace.indices.foreach { i =>
      floatValues1_inplace(i) = rand.nextFloat()
    }

    longValues1_inplace.indices.foreach { i =>
      longValues1_inplace(i) = rand.nextInt(10) + 1L
    }

    intValues1_inplace.indices.foreach { i =>
      intValues1_inplace(i) = rand.nextInt(10) + 1
    }


    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1))


    ilist.add(VFactory.denseFloatVector(densefloatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1))

    ilist.add(VFactory.denseLongVector(denselongValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices1, longValues1))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices1, longValues1))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices2, longValues1))

    ilist.add(VFactory.denseIntVector(denseintValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices1, intValues1))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices1, intValues1))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices2, intValues1))

    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))
    ilist.add(VFactory.intDummyVector(dim, intsortedIndices1))
    ilist.add(VFactory.intDummyVector(dim, intsortedIndices2))

    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))
    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1))

    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))
    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1))

    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))
    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1))

    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))
    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1))

    llist.add(VFactory.longDummyVector(dim, longsortedIndices))
    llist.add(VFactory.longDummyVector(dim, longsortedIndices1))
    llist.add(VFactory.longDummyVector(dim, longsortedIndices2))
  }
}

class BinaryCoverageTest {
  val capacity: Int = BinaryCoverageTest.capacity
  val dim: Int = BinaryCoverageTest.dim

  val intrandIndices: Array[Int] = BinaryCoverageTest.intrandIndices
  val longrandIndices: Array[Long] = BinaryCoverageTest.longrandIndices
  val intsortedIndices: Array[Int] = BinaryCoverageTest.intsortedIndices
  val longsortedIndices: Array[Long] = BinaryCoverageTest.longsortedIndices

  val intValues: Array[Int] = BinaryCoverageTest.intValues
  val longValues: Array[Long] = BinaryCoverageTest.longValues
  val floatValues: Array[Float] = BinaryCoverageTest.floatValues
  val doubleValues: Array[Double] = BinaryCoverageTest.doubleValues

  val denseintValues: Array[Int] = BinaryCoverageTest.denseintValues
  val denselongValues: Array[Long] = BinaryCoverageTest.denselongValues
  val densefloatValues: Array[Float] = BinaryCoverageTest.densefloatValues
  val densedoubleValues: Array[Double] = BinaryCoverageTest.densedoubleValues

  val capacity1: Int = 80000
  val intrandIndices1: Array[Int] = BinaryCoverageTest.intrandIndices1
  val longrandIndices1: Array[Long] = BinaryCoverageTest.longrandIndices1
  val intsortedIndices1: Array[Int] = BinaryCoverageTest.intsortedIndices1
  val longsortedIndices1: Array[Long] = BinaryCoverageTest.longsortedIndices1

  val intValues1: Array[Int] = BinaryCoverageTest.intValues1
  val longValues1: Array[Long] = BinaryCoverageTest.longValues1
  val floatValues1: Array[Float] = BinaryCoverageTest.floatValues1
  val doubleValues1: Array[Double] = BinaryCoverageTest.doubleValues1

  val intrandIndices2: Array[Int] = BinaryCoverageTest.intrandIndices2
  val intsortedIndices2: Array[Int] = BinaryCoverageTest.intsortedIndices2
  val longrandIndices2: Array[Long] = BinaryCoverageTest.longrandIndices2
  val longsortedIndices2: Array[Long] = BinaryCoverageTest.longsortedIndices2


  val intValues_inplace: Array[Int] = BinaryCoverageTest.intValues_inplace
  val longValues_inplace: Array[Long] = BinaryCoverageTest.longValues_inplace
  val floatValues_inplace: Array[Float] = BinaryCoverageTest.floatValues_inplace
  val doubleValues_inplace: Array[Double] = BinaryCoverageTest.doubleValues_inplace

  val denseintValues_inplace: Array[Int] = BinaryCoverageTest.denseintValues_inplace
  val denselongValues_inplace: Array[Long] = BinaryCoverageTest.denselongValues_inplace
  val densefloatValues_inplace: Array[Float] = BinaryCoverageTest.densefloatValues_inplace
  val densedoubleValues_inplace: Array[Double] = BinaryCoverageTest.densedoubleValues_inplace

  val intValues1_inplace: Array[Int] = BinaryCoverageTest.intValues1_inplace
  val longValues1_inplace: Array[Long] = BinaryCoverageTest.longValues1_inplace
  val floatValues1_inplace: Array[Float] = BinaryCoverageTest.floatValues1_inplace
  val doubleValues1_inplace: Array[Double] = BinaryCoverageTest.doubleValues1_inplace


  val ilist = BinaryCoverageTest.ilist
  val llist = BinaryCoverageTest.llist

  @Test
  def Addtest() {
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          (ilist.get(i).add(ilist.get(j))).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          (llist.get(i).add(llist.get(j))).sum()
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
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
         (ilist.get(i).sub(ilist.get(j))).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).sub(llist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
  }

  @Test
  def Multest() {
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
         ilist.get(i).mul(ilist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          llist.get(i).mul(llist.get(j)).sum()
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
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          ilist.get(i).div(ilist.get(j)).sum()
        } catch {
          case e: ArithmeticException =>{
            e
          }
          case e: AngelException => {
           e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          llist.get(i).div(llist.get(j)).sum()
        } catch {
          case e: ArithmeticException =>{
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
  def Axpytest() {
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          ilist.get(i).axpy(ilist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
         llist.get(i).axpy(llist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
  }

  @Test
  def Dottest() {
    llist.add(VFactory.longDummyVector(dim, longsortedIndices))
    ilist.add(VFactory.intDummyVector(dim, intrandIndices))
    llist.add(VFactory.longDummyVector(dim, longsortedIndices1))
    ilist.add(VFactory.intDummyVector(dim, intrandIndices1))
    ilist.add(VFactory.intDummyVector(dim, intrandIndices2))
    llist.add(VFactory.longDummyVector(dim, longsortedIndices2))
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          ilist.get(i).dot(ilist.get(j))
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          llist.get(i).dot(llist.get(j))
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
