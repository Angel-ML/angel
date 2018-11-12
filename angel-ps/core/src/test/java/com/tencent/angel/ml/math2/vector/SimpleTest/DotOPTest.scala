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
import breeze.linalg._
import breeze.numerics.abs
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}

object DotOPTest {
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

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    ilist.add(VFactory.denseFloatVector(densefloatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))

    ilist.add(VFactory.denseLongVector(denselongValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))

    ilist.add(VFactory.denseIntVector(denseintValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))

    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))


    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))

    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))

    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))

    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))

    llist.add(VFactory.longDummyVector(dim, longsortedIndices))
  }
}

// angel:28237, breeze:34695, ratio:1.2287070156178064
class DotOPTest {
  val capacity: Int = DotOPTest.capacity
  val dim: Int = DotOPTest.capacity * 100

  val times = 50
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val ilist = DotOPTest.ilist
  val llist = DotOPTest.llist

  val intrandIndices: Array[Int] = DotOPTest.intrandIndices
  val longrandIndices: Array[Long] = DotOPTest.longrandIndices
  val intsortedIndices: Array[Int] = DotOPTest.intsortedIndices
  val longsortedIndices: Array[Long] = DotOPTest.longsortedIndices

  val intValues: Array[Int] = DotOPTest.intValues
  val longValues: Array[Long] = DotOPTest.longValues
  val floatValues: Array[Float] = DotOPTest.floatValues
  val doubleValues: Array[Double] = DotOPTest.doubleValues

  val denseintValues: Array[Int] = DotOPTest.denseintValues
  val denselongValues: Array[Long] = DotOPTest.denselongValues
  val densefloatValues: Array[Float] = DotOPTest.densefloatValues
  val densedoubleValues: Array[Double] = DotOPTest.densedoubleValues

  @Test
  def dotIntKeyVectorTest() {
    val dense1 = DenseVector[Double](densedoubleValues)
    val hash1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => hash1(i) = v }
    val sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

    val dense2 = DenseVector[Float](densefloatValues)
    val hash2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => hash2(i) = v }
    val sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, 0.0f))

    val dense3 = DenseVector[Long](denselongValues)
    val hash3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => hash3(i) = v }
    val sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, 0l))

    val dense4 = DenseVector[Int](denseintValues)
    val hash4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => hash4(i) = v }
    val sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, 0))

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


    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} dot ${ilist.get(0).getClass.getSimpleName}: ${ilist.get(0).getClass.getSimpleName} is ${ilist.get(0).dot(ilist.get(0))}, and breeze is ${dense1.dot(dense1)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} dot ${ilist.get(1).getClass.getSimpleName}: ${ilist.get(1).getClass.getSimpleName} is ${ilist.get(1).dot(ilist.get(1))}, and breeze is ${hash1.dot(hash1)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} dot ${ilist.get(2).getClass.getSimpleName}: ${ilist.get(2).getClass.getSimpleName} is ${ilist.get(2).dot(ilist.get(2))}, and breeze is ${sorted1.dot(sorted1)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} dot ${ilist.get(3).getClass.getSimpleName}: ${ilist.get(3).getClass.getSimpleName} is ${ilist.get(3).dot(ilist.get(3))}, and breeze is ${dense2.dot(dense2)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} dot ${ilist.get(4).getClass.getSimpleName}: ${ilist.get(4).getClass.getSimpleName} is ${ilist.get(4).dot(ilist.get(4))}, and breeze is ${hash2.dot(hash2)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} dot ${ilist.get(5).getClass.getSimpleName}: ${ilist.get(5).getClass.getSimpleName} is ${ilist.get(5).dot(ilist.get(5))}, and breeze is ${sorted2.dot(sorted2)}")
    println(s"${ilist.get(6).getClass.getSimpleName}: ${getFlag(ilist.get(6))} dot ${ilist.get(6).getClass.getSimpleName}: ${ilist.get(6).getClass.getSimpleName} is ${ilist.get(6).dot(ilist.get(6))}, and breeze is ${dense3.dot(dense3)}")
    println(s"${ilist.get(7).getClass.getSimpleName}: ${getFlag(ilist.get(7))} dot ${ilist.get(7).getClass.getSimpleName}: ${ilist.get(7).getClass.getSimpleName} is ${ilist.get(7).dot(ilist.get(7))}, and breeze is ${hash3.dot(hash3)}")
    println(s"${ilist.get(8).getClass.getSimpleName}: ${getFlag(ilist.get(8))} dot ${ilist.get(8).getClass.getSimpleName}: ${ilist.get(8).getClass.getSimpleName} is ${ilist.get(8).dot(ilist.get(8))}, and breeze is ${sorted3.dot(sorted3)}")
    println(s"${ilist.get(9).getClass.getSimpleName}: ${getFlag(ilist.get(9))} dot ${ilist.get(9).getClass.getSimpleName}: ${ilist.get(9).getClass.getSimpleName} is ${ilist.get(9).dot(ilist.get(9))}, and breeze is ${dense4.dot(dense4)}")
    println(s"${ilist.get(10).getClass.getSimpleName}: ${getFlag(ilist.get(10))} dot ${ilist.get(10).getClass.getSimpleName}: ${ilist.get(10).getClass.getSimpleName} is ${ilist.get(10).dot(ilist.get(10))}, and breeze is ${hash4.dot(hash4)}")
    println(s"${ilist.get(11).getClass.getSimpleName}: ${getFlag(ilist.get(11))} dot ${ilist.get(11).getClass.getSimpleName}: ${ilist.get(11).getClass.getSimpleName} is ${ilist.get(11).dot(ilist.get(11))}, and breeze is ${sorted4.dot(sorted4)}")
    println(s"${ilist.get(12).getClass.getSimpleName}: ${getFlag(ilist.get(12))} dot ${ilist.get(12).getClass.getSimpleName}: ${ilist.get(12).getClass.getSimpleName} is ${ilist.get(12).dot(ilist.get(12))}, and breeze is ${intValues.length}")


    assert(abs(ilist.get(0).dot(ilist.get(0)) - dense1.dot(dense1)) < 1.0E-8)
    assert(abs(ilist.get(1).dot(ilist.get(1)) - hash1.dot(hash1)) < 1.0E-8)
    assert(abs(ilist.get(2).dot(ilist.get(2)) - sorted1.dot(sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).dot(ilist.get(3)) - dense2.dot(dense2)) < 1.0)
    assert(abs(ilist.get(4).dot(ilist.get(4)) - hash2.dot(hash2)) < 1.0E-3)
    assert(abs(ilist.get(5).dot(ilist.get(5)) - sorted2.dot(sorted2)) < 1.0E-3)
    assert(abs(ilist.get(6).dot(ilist.get(6)) - dense3.dot(dense3)) < 1.0E-8)
    assert(abs(ilist.get(7).dot(ilist.get(7)) - hash3.dot(hash3)) < 1.0E-8)
    assert(abs(ilist.get(8).dot(ilist.get(8)) - sorted3.dot(sorted3)) < 1.0E-8)
    assert(abs(ilist.get(9).dot(ilist.get(9)) - dense4.dot(dense4)) < 1.0E-8)
    assert(abs(ilist.get(10).dot(ilist.get(10)) - hash4.dot(hash4)) < 1.0E-8)
    assert(abs(ilist.get(11).dot(ilist.get(11)) - sorted4.dot(sorted4)) < 1.0E-8)
    assert(abs(ilist.get(12).dot(ilist.get(12)) - intValues.length) < 1.0E-8)


    //dense cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(0).dot(ilist.get(0))
      ilist.get(3).dot(ilist.get(3))
      ilist.get(6).dot(ilist.get(6))
      ilist.get(9).dot(ilist.get(9))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1.dot(dense1)
      dense2.dot(dense2)
      dense3.dot(dense3)
      dense4.dot(dense4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel dense dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(1).dot(ilist.get(1))
      ilist.get(4).dot(ilist.get(4))
      ilist.get(7).dot(ilist.get(7))
      ilist.get(10).dot(ilist.get(10))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      hash1.dot(hash1)
      hash2.dot(hash2)
      hash3.dot(hash3)
      hash4.dot(hash4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      ilist.get(2).dot(ilist.get(2))
      ilist.get(5).dot(ilist.get(5))
      ilist.get(8).dot(ilist.get(8))
      ilist.get(11).dot(ilist.get(11))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1.dot(sorted1)
      sorted2.dot(sorted2)
      sorted3.dot(sorted3)
      sorted4.dot(sorted4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
  }

  @Test
  def dotLongKeyVectorTest() {
    val hash1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => hash1(i) = v }
    val sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

    val hash2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => hash2(i) = v }
    val sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, 0.0f))

    val hash3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => hash3(i) = v }
    val sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, 0L))

    val hash4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => hash4(i) = v }
    val sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, 0))


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

    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} dot ${llist.get(0).getClass.getSimpleName}: ${llist.get(0).getClass.getSimpleName} is ${llist.get(0).dot(llist.get(0))}, and breeze is ${hash1.dot(hash1)}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} dot ${llist.get(1).getClass.getSimpleName}: ${llist.get(1).getClass.getSimpleName} is ${llist.get(1).dot(llist.get(1))}, and breeze is ${sorted1.dot(sorted1)}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} dot ${llist.get(2).getClass.getSimpleName}: ${llist.get(2).getClass.getSimpleName} is ${llist.get(2).dot(llist.get(2))}, and breeze is ${hash2.dot(hash2)}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} dot ${llist.get(3).getClass.getSimpleName}: ${llist.get(3).getClass.getSimpleName} is ${llist.get(3).dot(llist.get(3))}, and breeze is ${sorted2.dot(sorted2)}")
    println(s"${llist.get(4).getClass.getSimpleName}: ${getFlag(llist.get(4))} dot ${llist.get(4).getClass.getSimpleName}: ${llist.get(4).getClass.getSimpleName} is ${llist.get(4).dot(llist.get(4))}, and breeze is ${hash3.dot(hash3)}")
    println(s"${llist.get(5).getClass.getSimpleName}: ${getFlag(llist.get(5))} dot ${llist.get(5).getClass.getSimpleName}: ${llist.get(5).getClass.getSimpleName} is ${llist.get(5).dot(llist.get(5))}, and breeze is ${sorted3.dot(sorted3)}")
    println(s"${llist.get(6).getClass.getSimpleName}: ${getFlag(llist.get(6))} dot ${llist.get(6).getClass.getSimpleName}: ${llist.get(6).getClass.getSimpleName} is ${llist.get(6).dot(llist.get(6))}, and breeze is ${hash4.dot(hash4)}")
    println(s"${llist.get(7).getClass.getSimpleName}: ${getFlag(llist.get(7))} dot ${llist.get(7).getClass.getSimpleName}: ${llist.get(7).getClass.getSimpleName} is ${llist.get(7).dot(llist.get(7))}, and breeze is ${sorted4.dot(sorted4)}")
    println(s"${llist.get(8).getClass.getSimpleName}: ${getFlag(llist.get(8))} dot ${llist.get(8).getClass.getSimpleName}: ${llist.get(8).getClass.getSimpleName} is ${llist.get(8).dot(llist.get(8))}, and breeze is ${longValues.length}")


    assert(abs(llist.get(0).dot(llist.get(0)) - hash1.dot(hash1)) < 1.0E-8)
    assert(abs(llist.get(1).dot(llist.get(1)) - sorted1.dot(sorted1)) < 1.0E-8)
    assert(abs(llist.get(2).dot(llist.get(2)) - hash2.dot(hash2)) < 1.0E-3)
    assert(abs(llist.get(3).dot(llist.get(3)) - sorted2.dot(sorted2)) < 1.0E-3)
    assert(abs(llist.get(4).dot(llist.get(4)) - hash3.dot(hash3)) < 1.0E-8)
    assert(abs(llist.get(5).dot(llist.get(5)) - sorted3.dot(sorted3)) < 1.0E-8)
    assert(abs(llist.get(6).dot(llist.get(6)) - hash4.dot(hash4)) < 1.0E-8)
    assert(abs(llist.get(7).dot(llist.get(7)) - sorted4.dot(sorted4)) < 1.0E-8)
    assert(abs(llist.get(8).dot(llist.get(8)) - longValues.length) < 1.0E-8)

    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(0).dot(llist.get(0))
      llist.get(2).dot(llist.get(2))
      llist.get(4).dot(llist.get(4))
      llist.get(6).dot(llist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      hash1.dot(hash1)
      hash2.dot(hash2)
      hash3.dot(hash3)
      hash4.dot(hash4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(1).dot(llist.get(1))
      llist.get(3).dot(llist.get(3))
      llist.get(5).dot(llist.get(5))
      llist.get(7).dot(llist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1.dot(sorted1)
      sorted2.dot(sorted2)
      sorted3.dot(sorted3)
      sorted4.dot(sorted4)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted dot:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
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
