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
    println("angel add test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} add ${ilist.get(j).getClass.getSimpleName}: ${getFlag(ilist.get(j))} is ${(ilist.get(i).add(ilist.get(j))).sum()}")
          //          assert(abs((ilist.get(i).add(ilist.get(j))).sum()-(ilist.get(i).sum()+ilist.get(j).sum()))<1.0E-3)
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} add ${llist.get(j).getClass.getSimpleName}: ${getFlag(llist.get(j))} is ${(llist.get(i).add(llist.get(j))).sum()}")
          //          assert(abs((llist.get(i).add(llist.get(j))).sum()-(llist.get(i).sum()+llist.get(j).sum()))<1.0E-3)
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def Subtest() {
    println("angel sub test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} sub ${ilist.get(j).getClass.getSimpleName}: ${getFlag(ilist.get(j))} is ${(ilist.get(i).sub(ilist.get(j))).sum()}")
          //          assert(abs((ilist.get(i).sub(ilist.get(j))).sum()-(ilist.get(i).sum()-ilist.get(j).sum()))<1.0E-3)
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} sub ${llist.get(j).getClass.getSimpleName}: ${getFlag(llist.get(j))} is ${(llist.get(i).sub(llist.get(j))).sum()}")
          //          assert(abs((llist.get(i).sub(llist.get(j))).sum()-(llist.get(i).sum()-llist.get(j).sum()))<1.0E-3)
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def Multest() {
    println("angel mul test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).mul(ilist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).mul(llist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def Divtest() {
    println("angel div test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).div(ilist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).div(llist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def Axpytest() {
    println("angel axpy test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).axpy(ilist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).axpy(llist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
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
    println("angel dot test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).dot(ilist.get(j))}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).dot(llist.get(j))}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def Iaddtest() {
    println("angel iadd test--")
    (0 until ilist.size()).foreach { i =>
      if (i < 6) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iadd(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iadd(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iadd(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iadd(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iadd(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iadd(ilist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 12) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iadd(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iadd(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iadd(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iadd(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iadd(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iadd(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iadd(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iadd(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iadd(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iadd(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iadd(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iadd(ilist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 18) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iadd(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iadd(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iadd(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iadd(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iadd(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iadd(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iadd(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iadd(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iadd(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iadd(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iadd(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iadd(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.iadd(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.iadd(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.iadd(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.iadd(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.iadd(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.iadd(ilist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 27) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iadd(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iadd(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iadd(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iadd(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iadd(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iadd(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iadd(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iadd(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iadd(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iadd(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iadd(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iadd(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.iadd(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.iadd(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.iadd(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.iadd(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.iadd(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.iadd(ilist.get(i))).sum()},${sorted32.sum()}")
        var dense4 = VFactory.denseIntVector(denseintValues)
        var sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
        var sorted4 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)
        var sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
        var sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
        println(s"${dense4.getClass.getSimpleName}: ${getFlag(dense4)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense4.iadd(ilist.get(i))).sum()},${dense4.sum()}")
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse4.iadd(ilist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted4.iadd(ilist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse41.iadd(ilist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted41.iadd(ilist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} iadd ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted42.iadd(ilist.get(i))).sum()},${sorted42.sum()}")
      }
    }


    //longkey
    (0 until llist.size()).foreach { i =>
      if (i < 5) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iadd(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iadd(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iadd(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iadd(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iadd(llist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 10) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iadd(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iadd(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iadd(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iadd(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iadd(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iadd(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iadd(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iadd(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iadd(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iadd(llist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 15) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iadd(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iadd(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iadd(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iadd(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iadd(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iadd(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iadd(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iadd(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iadd(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iadd(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.iadd(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.iadd(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.iadd(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.iadd(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.iadd(llist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 23) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iadd(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iadd(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iadd(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iadd(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iadd(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iadd(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iadd(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iadd(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iadd(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iadd(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.iadd(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.iadd(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.iadd(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.iadd(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.iadd(llist.get(i))).sum()},${sorted32.sum()}")
        var sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
        var sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
        var sparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
        var sorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse4.iadd(llist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted4.iadd(llist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse41.iadd(llist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted41.iadd(llist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} iadd ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted42.iadd(llist.get(i))).sum()},${sorted42.sum()}")
      }
    }
  }

  @Test
  def Isubtest() {
    println("angel isub test--")
    (0 until ilist.size()).foreach { i =>
      if (i < 6) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.isub(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.isub(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.isub(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.isub(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.isub(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.isub(ilist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 12) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.isub(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.isub(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.isub(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.isub(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.isub(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.isub(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.isub(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.isub(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.isub(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.isub(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.isub(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.isub(ilist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 18) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.isub(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.isub(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.isub(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.isub(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.isub(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.isub(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.isub(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.isub(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.isub(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.isub(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.isub(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.isub(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.isub(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.isub(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.isub(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.isub(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.isub(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.isub(ilist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 27) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.isub(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.isub(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.isub(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.isub(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.isub(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.isub(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.isub(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.isub(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.isub(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.isub(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.isub(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.isub(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.isub(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.isub(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.isub(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.isub(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.isub(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.isub(ilist.get(i))).sum()},${sorted32.sum()}")
        var dense4 = VFactory.denseIntVector(denseintValues)
        var sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
        var sorted4 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)
        var sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
        var sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
        println(s"${dense4.getClass.getSimpleName}: ${getFlag(dense4)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense4.isub(ilist.get(i))).sum()},${dense4.sum()}")
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse4.isub(ilist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted4.isub(ilist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse41.isub(ilist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted41.isub(ilist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} isub ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted42.isub(ilist.get(i))).sum()},${sorted42.sum()}")
      }
    }


    //longkey
    (0 until llist.size()).foreach { i =>
      if (i < 5) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.isub(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.isub(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.isub(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.isub(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.isub(llist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 10) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.isub(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.isub(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.isub(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.isub(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.isub(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.isub(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.isub(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.isub(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.isub(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.isub(llist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 15) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.isub(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.isub(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.isub(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.isub(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.isub(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.isub(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.isub(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.isub(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.isub(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.isub(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.isub(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.isub(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.isub(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.isub(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.isub(llist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 23) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.isub(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.isub(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.isub(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.isub(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.isub(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.isub(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.isub(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.isub(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.isub(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.isub(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.isub(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.isub(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.isub(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.isub(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.isub(llist.get(i))).sum()},${sorted32.sum()}")
        var sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
        var sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
        var sparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
        var sorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse4.isub(llist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted4.isub(llist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse41.isub(llist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted41.isub(llist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} isub ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted42.isub(llist.get(i))).sum()},${sorted42.sum()}")
      }
    }
  }

  @Test
  def Imultest() {
    println("angel imul test--")
    (0 until ilist.size()).foreach { i =>
      if (i < 6) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.imul(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.imul(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.imul(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.imul(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.imul(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.imul(ilist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 12) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.imul(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.imul(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.imul(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.imul(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.imul(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.imul(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.imul(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.imul(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.imul(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.imul(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.imul(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.imul(ilist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 18) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.imul(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.imul(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.imul(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.imul(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.imul(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.imul(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.imul(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.imul(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.imul(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.imul(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.imul(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.imul(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.imul(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.imul(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.imul(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.imul(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.imul(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.imul(ilist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 27) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.imul(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.imul(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.imul(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.imul(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.imul(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.imul(ilist.get(i))).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.imul(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.imul(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.imul(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.imul(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.imul(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.imul(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
        var sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.imul(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.imul(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.imul(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.imul(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.imul(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.imul(ilist.get(i))).sum()},${sorted32.sum()}")
        var dense4 = VFactory.denseIntVector(denseintValues)
        var sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
        var sorted4 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)
        var sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1)
        var sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1)
        println(s"${dense4.getClass.getSimpleName}: ${getFlag(dense4)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense4.imul(ilist.get(i))).sum()},${dense4.sum()}")
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse4.imul(ilist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted4.imul(ilist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse41.imul(ilist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted41.imul(ilist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} imul ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted42.imul(ilist.get(i))).sum()},${sorted42.sum()}")
      }
    }


    //longkey
    (0 until llist.size()).foreach { i =>
      if (i < 5) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.imul(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.imul(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.imul(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.imul(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.imul(llist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 10) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)


        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.imul(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.imul(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.imul(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.imul(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.imul(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.imul(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.imul(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.imul(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.imul(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.imul(llist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 15) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.imul(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.imul(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.imul(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.imul(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.imul(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.imul(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.imul(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.imul(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.imul(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.imul(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.imul(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.imul(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.imul(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.imul(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.imul(llist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 23) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1)


        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.imul(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.imul(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.imul(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.imul(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.imul(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.imul(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.imul(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.imul(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.imul(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.imul(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.imul(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.imul(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.imul(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.imul(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.imul(llist.get(i))).sum()},${sorted32.sum()}")
        var sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
        var sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
        var sparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1)
        var sorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1)
        var sorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1)
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse4.imul(llist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted4.imul(llist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse41.imul(llist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted41.imul(llist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} imul ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted42.imul(llist.get(i))).sum()},${sorted42.sum()}")
      }
    }
  }

  @Test
  def Idivtest() {
    println("angel idiv test--")
    (0 until ilist.size()).foreach { i =>
      if (i < 6) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.idiv(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.idiv(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.idiv(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.idiv(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.idiv(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.idiv(ilist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 12) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.idiv(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.idiv(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.idiv(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.idiv(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.idiv(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.idiv(ilist.get(i))).sum()},${sorted12.sum()}")

        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.idiv(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.idiv(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.idiv(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.idiv(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.idiv(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.idiv(ilist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 18) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.idiv(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.idiv(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.idiv(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.idiv(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.idiv(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.idiv(ilist.get(i))).sum()},${sorted12.sum()}")

        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.idiv(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.idiv(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.idiv(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.idiv(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.idiv(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.idiv(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues_inplace)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1_inplace)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.idiv(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.idiv(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.idiv(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.idiv(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.idiv(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.idiv(ilist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 27) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.idiv(ilist.get(i))).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.idiv(ilist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.idiv(ilist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.idiv(ilist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.idiv(ilist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.idiv(ilist.get(i))).sum()},${sorted12.sum()}")

        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.idiv(ilist.get(i))).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.idiv(ilist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.idiv(ilist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.idiv(ilist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.idiv(ilist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.idiv(ilist.get(i))).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues_inplace)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1_inplace)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.idiv(ilist.get(i))).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.idiv(ilist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.idiv(ilist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.idiv(ilist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.idiv(ilist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.idiv(ilist.get(i))).sum()},${sorted32.sum()}")
        var dense4 = VFactory.denseIntVector(denseintValues_inplace)
        var sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues_inplace)
        var sorted4 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues_inplace)
        var sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1_inplace)
        var sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1_inplace)
        var sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1_inplace)
        println(s"${dense4.getClass.getSimpleName}: ${getFlag(dense4)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense4.idiv(ilist.get(i))).sum()},${dense4.sum()}")
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse4.idiv(ilist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted4.idiv(ilist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse41.idiv(ilist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted41.idiv(ilist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} idiv ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted42.idiv(ilist.get(i))).sum()},${sorted42.sum()}")
      }
    }


    //longkey
    (0 until llist.size()).foreach { i =>
      if (i < 5) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.idiv(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.idiv(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.idiv(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.idiv(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.idiv(llist.get(i))).sum()},${sorted12.sum()}")

      } else if (i < 10) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.idiv(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.idiv(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.idiv(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.idiv(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.idiv(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.idiv(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.idiv(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.idiv(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.idiv(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.idiv(llist.get(i))).sum()},${sorted22.sum()}")
      } else if (i < 15) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.idiv(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.idiv(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.idiv(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.idiv(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.idiv(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.idiv(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.idiv(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.idiv(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.idiv(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.idiv(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1_inplace)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.idiv(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.idiv(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.idiv(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.idiv(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.idiv(llist.get(i))).sum()},${sorted32.sum()}")
      } else if (i < 23) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.idiv(llist.get(i))).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.idiv(llist.get(i))).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.idiv(llist.get(i))).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.idiv(llist.get(i))).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.idiv(llist.get(i))).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.idiv(llist.get(i))).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.idiv(llist.get(i))).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.idiv(llist.get(i))).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.idiv(llist.get(i))).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.idiv(llist.get(i))).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1_inplace)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.idiv(llist.get(i))).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.idiv(llist.get(i))).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.idiv(llist.get(i))).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.idiv(llist.get(i))).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.idiv(llist.get(i))).sum()},${sorted32.sum()}")
        var sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues_inplace)
        var sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues_inplace)
        var sparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1_inplace)
        var sorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1_inplace)
        var sorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1_inplace)
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse4.idiv(llist.get(i))).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted4.idiv(llist.get(i))).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse41.idiv(llist.get(i))).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted41.idiv(llist.get(i))).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} idiv ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted42.idiv(llist.get(i))).sum()},${sorted42.sum()}")
      }
    }
  }

  @Test
  def Iaxpytest() {
    println("angel iaxpy test--")
    (0 until ilist.size()).foreach { i =>
      if (i < 6) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iaxpy(ilist.get(i), 2.0)).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iaxpy(ilist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iaxpy(ilist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iaxpy(ilist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iaxpy(ilist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iaxpy(ilist.get(i), 2.0)).sum()},${sorted12.sum()}")

      } else if (i < 12) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iaxpy(ilist.get(i), 2.0)).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iaxpy(ilist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iaxpy(ilist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iaxpy(ilist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iaxpy(ilist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iaxpy(ilist.get(i), 2.0)).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iaxpy(ilist.get(i), 2.0)).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iaxpy(ilist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iaxpy(ilist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iaxpy(ilist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iaxpy(ilist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iaxpy(ilist.get(i), 2.0)).sum()},${sorted22.sum()}")
      } else if (i < 18) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iaxpy(ilist.get(i), 2.0)).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iaxpy(ilist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iaxpy(ilist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iaxpy(ilist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iaxpy(ilist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iaxpy(ilist.get(i), 2.0)).sum()},${sorted12.sum()}")

        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iaxpy(ilist.get(i), 2.0)).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iaxpy(ilist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iaxpy(ilist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iaxpy(ilist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iaxpy(ilist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iaxpy(ilist.get(i), 2.0)).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues_inplace)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1_inplace)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.iaxpy(ilist.get(i), 2.0)).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.iaxpy(ilist.get(i), 2.0)).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.iaxpy(ilist.get(i), 2.0)).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.iaxpy(ilist.get(i), 2.0)).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.iaxpy(ilist.get(i), 2.0)).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.iaxpy(ilist.get(i), 2.0)).sum()},${sorted32.sum()}")
      } else if (i < 27) {
        var dense1 = VFactory.denseDoubleVector(densedoubleValues_inplace)
        var sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseDoubleVector(dim, intrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedDoubleVector(dim, capacity1, intsortedIndices2, doubleValues1_inplace)
        println(s"${dense1.getClass.getSimpleName}: ${getFlag(dense1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense1.iaxpy(ilist.get(i), 2.0)).sum()},${dense1.sum()}")
        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse1.iaxpy(ilist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted1.iaxpy(ilist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse11.iaxpy(ilist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted11.iaxpy(ilist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted12.iaxpy(ilist.get(i), 2.0)).sum()},${sorted12.sum()}")
        var dense2 = VFactory.denseFloatVector(densefloatValues_inplace)
        var sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseFloatVector(dim, intrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedFloatVector(dim, intsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedFloatVector(dim, intsortedIndices2, floatValues1_inplace)
        println(s"${dense2.getClass.getSimpleName}: ${getFlag(dense2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense2.iaxpy(ilist.get(i), 2.0)).sum()},${dense2.sum()}")
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse2.iaxpy(ilist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted2.iaxpy(ilist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse21.iaxpy(ilist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted21.iaxpy(ilist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted22.iaxpy(ilist.get(i), 2.0)).sum()},${sorted22.sum()}")
        var dense3 = VFactory.denseLongVector(denselongValues_inplace)
        var sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongVector(dim, intrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongVector(dim, intsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongVector(dim, intsortedIndices2, longValues1_inplace)
        println(s"${dense3.getClass.getSimpleName}: ${getFlag(dense3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense3.iaxpy(ilist.get(i), 2.0)).sum()},${dense3.sum()}")
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse3.iaxpy(ilist.get(i), 2.0)).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted3.iaxpy(ilist.get(i), 2.0)).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse31.iaxpy(ilist.get(i), 2.0)).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted31.iaxpy(ilist.get(i), 2.0)).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted32.iaxpy(ilist.get(i), 2.0)).sum()},${sorted32.sum()}")
        var dense4 = VFactory.denseIntVector(denseintValues_inplace)
        var sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues_inplace)
        var sorted4 = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues_inplace)
        var sparse41 = VFactory.sparseIntVector(dim, intrandIndices1, intValues1_inplace)
        var sorted41 = VFactory.sortedIntVector(dim, intsortedIndices1, intValues1_inplace)
        var sorted42 = VFactory.sortedIntVector(dim, intsortedIndices2, intValues1_inplace)
        println(s"${dense4.getClass.getSimpleName}: ${getFlag(dense4)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(dense4.iaxpy(ilist.get(i), 2.0)).sum()},${dense4.sum()}")
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse4.iaxpy(ilist.get(i), 2.0)).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted4.iaxpy(ilist.get(i), 2.0)).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sparse41.iaxpy(ilist.get(i), 2.0)).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted41.iaxpy(ilist.get(i), 2.0)).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} iaxpy ${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} is ${(sorted42.iaxpy(ilist.get(i), 2.0)).sum()},${sorted42.sum()}")
      }
    }


    //longkey
    (0 until llist.size()).foreach { i =>
      if (i < 5) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iaxpy(llist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iaxpy(llist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iaxpy(llist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iaxpy(llist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iaxpy(llist.get(i), 2.0)).sum()},${sorted12.sum()}")

      } else if (i < 10) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iaxpy(llist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iaxpy(llist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iaxpy(llist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iaxpy(llist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iaxpy(llist.get(i), 2.0)).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iaxpy(llist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iaxpy(llist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iaxpy(llist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iaxpy(llist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iaxpy(llist.get(i), 2.0)).sum()},${sorted22.sum()}")
      } else if (i < 15) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iaxpy(llist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iaxpy(llist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iaxpy(llist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iaxpy(llist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iaxpy(llist.get(i), 2.0)).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iaxpy(llist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iaxpy(llist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iaxpy(llist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iaxpy(llist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iaxpy(llist.get(i), 2.0)).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1_inplace)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.iaxpy(llist.get(i), 2.0)).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.iaxpy(llist.get(i), 2.0)).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.iaxpy(llist.get(i), 2.0)).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.iaxpy(llist.get(i), 2.0)).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.iaxpy(llist.get(i), 2.0)).sum()},${sorted32.sum()}")
      } else if (i < 23) {
        var sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues_inplace)
        var sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues_inplace)
        var sparse11 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices1, doubleValues1_inplace)
        var sorted11 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices1, doubleValues1_inplace)
        var sorted12 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices2, doubleValues1_inplace)

        println(s"${sparse1.getClass.getSimpleName}: ${getFlag(sparse1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse1.iaxpy(llist.get(i), 2.0)).sum()},${sparse1.sum()}")
        println(s"${sorted1.getClass.getSimpleName}: ${getFlag(sorted1)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted1.iaxpy(llist.get(i), 2.0)).sum()},${sorted1.sum()}")
        println(s"${sparse11.getClass.getSimpleName}: ${getFlag(sparse11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse11.iaxpy(llist.get(i), 2.0)).sum()},${sparse11.sum()}")
        println(s"${sorted11.getClass.getSimpleName}: ${getFlag(sorted11)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted11.iaxpy(llist.get(i), 2.0)).sum()},${sorted11.sum()}")
        println(s"${sorted12.getClass.getSimpleName}: ${getFlag(sorted12)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted12.iaxpy(llist.get(i), 2.0)).sum()},${sorted12.sum()}")

        var sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues_inplace)
        var sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues_inplace)
        var sparse21 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices1, floatValues1_inplace)
        var sorted21 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices1, floatValues1_inplace)
        var sorted22 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices2, floatValues1_inplace)
        println(s"${sparse2.getClass.getSimpleName}: ${getFlag(sparse2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse2.iaxpy(llist.get(i), 2.0)).sum()},${sparse2.sum()}")
        println(s"${sorted2.getClass.getSimpleName}: ${getFlag(sorted2)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted2.iaxpy(llist.get(i), 2.0)).sum()},${sorted2.sum()}")
        println(s"${sparse21.getClass.getSimpleName}: ${getFlag(sparse21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse21.iaxpy(llist.get(i), 2.0)).sum()},${sparse21.sum()}")
        println(s"${sorted21.getClass.getSimpleName}: ${getFlag(sorted21)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted21.iaxpy(llist.get(i), 2.0)).sum()},${sorted21.sum()}")
        println(s"${sorted22.getClass.getSimpleName}: ${getFlag(sorted22)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted22.iaxpy(llist.get(i), 2.0)).sum()},${sorted22.sum()}")
        var sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues_inplace)
        var sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues_inplace)
        var sparse31 = VFactory.sparseLongKeyLongVector(dim, longrandIndices1, longValues1_inplace)
        var sorted31 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices1, longValues1_inplace)
        var sorted32 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices2, longValues1_inplace)
        println(s"${sparse3.getClass.getSimpleName}: ${getFlag(sparse3)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse3.iaxpy(llist.get(i), 2.0)).sum()},${sparse3.sum()}")
        println(s"${sorted3.getClass.getSimpleName}: ${getFlag(sorted3)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted3.iaxpy(llist.get(i), 2.0)).sum()},${sorted3.sum()}")
        println(s"${sparse31.getClass.getSimpleName}: ${getFlag(sparse31)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse31.iaxpy(llist.get(i), 2.0)).sum()},${sparse31.sum()}")
        println(s"${sorted31.getClass.getSimpleName}: ${getFlag(sorted31)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted31.iaxpy(llist.get(i), 2.0)).sum()},${sorted31.sum()}")
        println(s"${sorted32.getClass.getSimpleName}: ${getFlag(sorted32)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted32.iaxpy(llist.get(i), 2.0)).sum()},${sorted32.sum()}")
        var sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues_inplace)
        var sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues_inplace)
        var sparse41 = VFactory.sparseLongKeyIntVector(dim, longrandIndices1, intValues1_inplace)
        var sorted41 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices1, intValues1_inplace)
        var sorted42 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices2, intValues1_inplace)
        println(s"${sparse4.getClass.getSimpleName}: ${getFlag(sparse4)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse4.iaxpy(llist.get(i), 2.0)).sum()},${sparse4.sum()}")
        println(s"${sorted4.getClass.getSimpleName}: ${getFlag(sorted4)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted4.iaxpy(llist.get(i), 2.0)).sum()},${sorted4.sum()}")
        println(s"${sparse41.getClass.getSimpleName}: ${getFlag(sparse41)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sparse41.iaxpy(llist.get(i), 2.0)).sum()},${sparse41.sum()}")
        println(s"${sorted41.getClass.getSimpleName}: ${getFlag(sorted41)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted41.iaxpy(llist.get(i), 2.0)).sum()},${sorted41.sum()}")
        println(s"${sorted42.getClass.getSimpleName}: ${getFlag(sorted42)} iaxpy ${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} is ${(sorted42.iaxpy(llist.get(i), 2.0)).sum()},${sorted42.sum()}")
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
