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
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}

object BinaryLongKeyOPTest {
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
  val longdummyValues: Array[Int] = new Array[Int](capacity)

  val denseintValues: Array[Int] = new Array[Int](dim)
  val denselongValues: Array[Long] = new Array[Long](dim)
  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)

  val llist = new util.ArrayList[Vector]()

  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0F))

  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0F))

  var sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))

  var sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

  var longdummy = new SparseVector[Int](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))

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
      longValues(i) = rand.nextInt(100)
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(100)
    }

    longdummyValues.indices.foreach { i =>
      longdummyValues(i) = 1
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

    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))

    sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse3(i) = v }
    sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))

    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))

    llist.add(VFactory.longDummyVector(dim, longsortedIndices))

    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    longdummy = new SparseVector[Int](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))
  }
}

class BinaryLongKeyOPTest {
  val llist = BinaryLongKeyOPTest.llist

  var sparse1 = BinaryLongKeyOPTest.sparse1
  var sorted1 = BinaryLongKeyOPTest.sorted1

  var sparse2 = BinaryLongKeyOPTest.sparse2
  var sorted2 = BinaryLongKeyOPTest.sorted2

  var sparse3 = BinaryLongKeyOPTest.sparse3
  var sorted3 = BinaryLongKeyOPTest.sorted3

  var sparse4 = BinaryLongKeyOPTest.sparse4
  var sorted4 = BinaryLongKeyOPTest.sorted4

  var longdummy = BinaryLongKeyOPTest.longdummy

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def Addtest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(0).add(llist.get(0))
      llist.get(2).add(llist.get(2))
      llist.get(4).add(llist.get(4))
      llist.get(6).add(llist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 + sparse1
      sparse2 + sparse2
      sparse3 + sparse3
      sparse4 + sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(1).add(llist.get(1))
      llist.get(3).add(llist.get(3))
      llist.get(5).add(llist.get(5))
      llist.get(7).add(llist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 + sorted1
      sorted2 + sorted2
      sorted3 + sorted3
      sorted4 + sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted add:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println("angel add test--")
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} add ${llist.get(j).getClass.getSimpleName}: ${getFlag(llist.get(j))} is ${(llist.get(i).add(llist.get(j))).sum()}")
          if (getFlag(llist.get(i)) != "dummy") {
            assert(abs((llist.get(i).add(llist.get(j))).sum() - (llist.get(i).sum() + llist.get(j).sum())) < 1.0E-3)
          } else {
            assert(abs((llist.get(i).add(llist.get(j))).sum() - (llist.get(i).sum() + sum(longdummy))) < 1.0E-3)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }

    assert(abs((llist.get(0).add(llist.get(0))).sum() - sum(sparse1 + sparse1)) < 1.0E-8)
    assert(abs((llist.get(1).add(llist.get(1))).sum() - sum(sorted1 + sorted1)) < 1.0E-8)
    assert(abs((llist.get(2).add(llist.get(2))).sum() - sum(sparse2 + sparse2)) < 1.0E-3)
    assert(abs((llist.get(3).add(llist.get(3))).sum() - sum(sorted2 + sorted2)) < 1.0E-3)
    assert(abs((llist.get(4).add(llist.get(4))).sum() - sum(sparse3 + sparse3)) < 1.0E-8)
    assert(abs((llist.get(5).add(llist.get(5))).sum() - sum(sorted3 + sorted3)) < 1.0E-8)
    assert(abs((llist.get(6).add(llist.get(6))).sum() - sum(sparse4 + sparse4)) < 1.0E-8)
    assert(abs((llist.get(7).add(llist.get(7))).sum() - sum(sorted4 + sorted4)) < 1.0E-8)

    println("angel iadd test--")
    val isparse1 = llist.get(0).iadd(llist.get(0))
    val isorted1 = llist.get(1).iadd(llist.get(1))
    val isparse2 = llist.get(2).iadd(llist.get(2))
    val isorted2 = llist.get(3).iadd(llist.get(3))
    val isparse3 = llist.get(4).iadd(llist.get(4))
    val isorted3 = llist.get(5).iadd(llist.get(5))
    val isparse4 = llist.get(6).iadd(llist.get(6))
    val isorted4 = llist.get(7).iadd(llist.get(7))


    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())
    assert((llist.get(4)).sum() == (isparse3).sum())
    assert((llist.get(5)).sum() == (isorted3).sum())
    assert((llist.get(6)).sum() == (isparse4).sum())
    assert((llist.get(7)).sum() == (isorted4).sum())
  }

  @Test
  def Subtest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(0).sub(llist.get(0))
      llist.get(2).sub(llist.get(2))
      llist.get(4).sub(llist.get(4))
      llist.get(6).sub(llist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 - sparse1
      sparse2 - sparse2
      sparse3 - sparse3
      sparse4 - sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(1).sub(llist.get(1))
      llist.get(3).sub(llist.get(3))
      llist.get(5).sub(llist.get(5))
      llist.get(7).sub(llist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 - sorted1
      sorted2 - sorted2
      sorted3 - sorted3
      sorted4 - sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sub:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println("angel sub test--")
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} sub ${llist.get(j).getClass.getSimpleName}: ${getFlag(llist.get(j))} is ${(llist.get(i).sub(llist.get(j))).sum()}")
          if (getFlag(llist.get(i)) != "dummy") {
            assert(abs((llist.get(i).sub(llist.get(j))).sum() - (llist.get(i).sum() - llist.get(j).sum())) < 1.0E-3)
          } else {
            assert(abs((llist.get(i).sub(llist.get(j))).sum() - (llist.get(i).sum() - sum(longdummy))) < 1.0E-3)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    assert((llist.get(0).sub(llist.get(0))).sum() == sum(sparse1 - sparse1))
    assert((llist.get(1).sub(llist.get(1))).sum() == sum(sorted1 - sorted1))
    assert((llist.get(2).sub(llist.get(2))).sum() == sum(sparse2 - sparse2))
    assert((llist.get(3).sub(llist.get(3))).sum() == sum(sorted2 - sorted2))
    assert((llist.get(4).sub(llist.get(4))).sum() == sum(sparse3 - sparse3))
    assert((llist.get(5).sub(llist.get(5))).sum() == sum(sorted3 - sorted3))
    assert((llist.get(6).sub(llist.get(6))).sum() == sum(sparse4 - sparse4))
    assert((llist.get(7).sub(llist.get(7))).sum() == sum(sorted4 - sorted4))

    println("angel isub test--")
    val isparse1 = llist.get(0).isub(llist.get(0))
    val isorted1 = llist.get(1).isub(llist.get(1))
    val isparse2 = llist.get(2).isub(llist.get(2))
    val isorted2 = llist.get(3).isub(llist.get(3))
    val isparse3 = llist.get(4).isub(llist.get(4))
    val isorted3 = llist.get(5).isub(llist.get(5))
    val isparse4 = llist.get(6).isub(llist.get(6))
    val isorted4 = llist.get(7).isub(llist.get(7))


    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())
    assert((llist.get(4)).sum() == (isparse3).sum())
    assert((llist.get(5)).sum() == (isorted3).sum())
    assert((llist.get(6)).sum() == (isparse4).sum())
    assert((llist.get(7)).sum() == (isorted4).sum())
  }

  @Test
  def Multest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(0).mul(llist.get(0))
      llist.get(2).mul(llist.get(2))
      llist.get(4).mul(llist.get(4))
      llist.get(6).mul(llist.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :* sparse1
      sparse2 :* sparse2
      sparse3 :* sparse3
      sparse4 :* sparse4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      llist.get(1).mul(llist.get(1))
      llist.get(3).mul(llist.get(3))
      llist.get(5).mul(llist.get(5))
      llist.get(7).mul(llist.get(7))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :* sorted1
      sorted2 :* sorted2
      sorted3 :* sorted3
      sorted4 :* sorted4
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted mul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println("angel mul test--")

    assert(abs((llist.get(0).mul(llist.get(0))).sum() - sum(sparse1 :* sparse1)) < 1.0E-8)
    assert(abs((llist.get(1).mul(llist.get(1))).sum() - sum(sorted1 :* sorted1)) < 1.0E-8)
    assert(abs((llist.get(2).mul(llist.get(2))).sum() - sum(sparse2 :* sparse2)) < 1.0E-3)
    assert(abs((llist.get(3).mul(llist.get(3))).sum() - sum(sorted2 :* sorted2)) < 1.0E-3)
    assert(abs((llist.get(4).mul(llist.get(4))).sum() - sum(sparse3 :* sparse3)) < 1.0E-8)
    assert(abs((llist.get(5).mul(llist.get(5))).sum() - sum(sorted3 :* sorted3)) < 1.0E-8)
    assert(abs((llist.get(6).mul(llist.get(6))).sum() - sum(sparse4 :* sparse4)) < 1.0E-8)
    assert(abs((llist.get(7).mul(llist.get(7))).sum() - sum(sorted4 :* sorted4)) < 1.0E-8)

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

    println("angel imul test--")
    val isparse1 = llist.get(0).imul(llist.get(0))
    val isorted1 = llist.get(1).imul(llist.get(1))
    val isparse2 = llist.get(2).imul(llist.get(2))
    val isorted2 = llist.get(3).imul(llist.get(3))
    val isparse3 = llist.get(4).imul(llist.get(4))
    val isorted3 = llist.get(5).imul(llist.get(5))
    val isparse4 = llist.get(6).imul(llist.get(6))
    val isorted4 = llist.get(7).imul(llist.get(7))


    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())
    assert((llist.get(4)).sum() == (isparse3).sum())
    assert((llist.get(5)).sum() == (isorted3).sum())
    assert((llist.get(6)).sum() == (isparse4).sum())
    assert((llist.get(7)).sum() == (isorted4).sum())
  }

  @Test
  def Divtest() {
    //sparse cost
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      llist.get(0).div(llist.get(0))
    //      llist.get(2).div(llist.get(2))
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sparse1 :/ sparse1
    //      sparse2 :/ sparse2
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    //sorted cost
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      llist.get(1).div(llist.get(1))
    //      llist.get(3).div(llist.get(3))
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sorted1 :/ sorted1
    //      sorted2 :/ sorted2
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted div:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    println("angel div test--")//div运算在sparse和sorted中的运算结果与breeze不同
    //    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} div ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${(llist.get(0).div(llist.get(0))).sum()}, and breeze is ${sum(sparse1 :/ sparse1)}")
    //    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} div ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${(llist.get(1).div(llist.get(1))).sum()}, and breeze is ${sum(sorted1 :/ sorted1)}")
    //    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} div ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${(llist.get(2).div(llist.get(2))).sum()}, and breeze is ${sum(sparse2 :/ sparse2)}")
    //    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} div ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${(llist.get(3).div(llist.get(3))).sum()}, and breeze is ${sum(sorted2 :/ sorted2)}")
    //    println(s"${llist.get(4).getClass.getSimpleName}: ${getFlag(llist.get(4))} div ${llist.get(4).getClass.getSimpleName}: ${getFlag(llist.get(4))} is ${(llist.get(4).div(llist.get(4))).sum()}, and breeze is ${sum(sparse3 :/ sparse3)}")
    //    println(s"${llist.get(5).getClass.getSimpleName}: ${getFlag(llist.get(5))} div ${llist.get(5).getClass.getSimpleName}: ${getFlag(llist.get(5))} is ${(llist.get(5).div(llist.get(5))).sum()}, and breeze is ${sum(sorted3 :/ sorted3)}")
    //    println(s"${llist.get(6).getClass.getSimpleName}: ${getFlag(llist.get(6))} div ${llist.get(6).getClass.getSimpleName}: ${getFlag(llist.get(6))} is ${(llist.get(6).div(llist.get(6))).sum()}, and breeze is ${sum(sparse4 :/ sparse4)}")
    //    println(s"${llist.get(7).getClass.getSimpleName}: ${getFlag(llist.get(7))} div ${llist.get(7).getClass.getSimpleName}: ${getFlag(llist.get(7))} is ${(llist.get(7).div(llist.get(7))).sum()}, and breeze is ${sum(sorted4 :/ sorted4)}")


    //    assert((llist.get(0).div(llist.get(0))).sum() == sum(sparse1 :/ sparse1))
    //    assert((llist.get(1).div(llist.get(1))).sum() == sum(sorted1 :/ sorted1))
    //    assert((llist.get(2).div(llist.get(2))).sum() == sum(sparse2 :/ sparse2))
    //    assert((llist.get(3).div(llist.get(3))).sum() == sum(sorted2 :/ sorted2))
    //    assert((llist.get(4).div(llist.get(4))).sum() == sum(sparse3 :/ sparse3))
    //    assert((llist.get(5).div(llist.get(5))).sum() == sum(sorted3 :/ sorted3))
    //    assert((llist.get(6).div(llist.get(6))).sum() == sum(sparse4 :/ sparse4))
    //    assert((llist.get(7).div(llist.get(7))).sum() == sum(sorted4 :/ sorted4))

    (0 until llist.size()).foreach { i =>
      (i + 1 until llist.size()).foreach { j =>
        println(s"$i, $j, ${llist.get(i).div(llist.get(j)).sum()}")
        //        try{
        //          println(s"${llist.get(i).div(llist.get(j)).sum()}")
        //        }catch{
        //          case e: AngelException =>{
        //            println(e)
        //          }
        //        }
      }
    }

    val isparse1 = llist.get(0).idiv(llist.get(0))
    val isorted1 = llist.get(1).idiv(llist.get(1))
    val isparse2 = llist.get(2).idiv(llist.get(2))
    val isorted2 = llist.get(3).idiv(llist.get(3))
    val isparse3 = llist.get(4).idiv(llist.get(4))
    val isorted3 = llist.get(5).idiv(llist.get(5))
    val isparse4 = llist.get(6).idiv(llist.get(6))
    val isorted4 = llist.get(7).idiv(llist.get(7))


    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())
    assert((llist.get(4)).sum() == (isparse3).sum())
    assert((llist.get(5)).sum() == (isorted3).sum())
    assert((llist.get(6)).sum() == (isparse4).sum())
    assert((llist.get(7)).sum() == (isorted4).sum())
  }

  @Test
  def Axpytest() {
    println("angel axpy test--")
    (0 until llist.size()).foreach { i =>
      (0 until llist.size()).foreach { j =>
        try {
          println(s"${llist.get(i).getClass.getSimpleName}: ${getFlag(llist.get(i))} axpy ${llist.get(j).getClass.getSimpleName}: ${getFlag(llist.get(j))} is ${(llist.get(i).axpy(llist.get(j), 2.0)).sum()}")
          assert(abs((llist.get(i).axpy(llist.get(j), 2.0)).sum() - (llist.get(i).sum() + llist.get(j).sum() * 2)) < 1.0E-3)
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }

    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} axpy ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${(llist.get(0).axpy(llist.get(0), 2.0)).sum()}, and breeze is ${sum(sparse1 + sparse1 * 2.0)},${sum(sparse1)}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} axpy ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${(llist.get(1).axpy(llist.get(1), 2.0)).sum()}, and breeze is ${sum(sorted1 + sorted1 * 2.0)}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} axpy ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${(llist.get(2).axpy(llist.get(2), 2.0)).sum()}, and breeze is ${sum(sparse2 + sparse2 * 2.0f)}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} axpy ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${(llist.get(3).axpy(llist.get(3), 2.0)).sum()}, and breeze is ${sum(sorted2 + sorted2 * 2.0f)}")
    println(s"${llist.get(4).getClass.getSimpleName}: ${getFlag(llist.get(4))} axpy ${llist.get(4).getClass.getSimpleName}: ${getFlag(llist.get(4))} is ${(llist.get(4).axpy(llist.get(4), 2.0)).sum()}, and breeze is ${sum(sparse3 + sparse3 * 2l)}")
    println(s"${llist.get(5).getClass.getSimpleName}: ${getFlag(llist.get(5))} axpy ${llist.get(5).getClass.getSimpleName}: ${getFlag(llist.get(5))} is ${(llist.get(5).axpy(llist.get(5), 2.0)).sum()}, and breeze is ${sum(sorted3 + sorted3 * 2l)}")
    println(s"${llist.get(6).getClass.getSimpleName}: ${getFlag(llist.get(6))} axpy ${llist.get(6).getClass.getSimpleName}: ${getFlag(llist.get(6))} is ${(llist.get(6).axpy(llist.get(6), 2.0)).sum()}, and breeze is ${sum(sparse4 + sparse4 * 2)}")
    println(s"${llist.get(7).getClass.getSimpleName}: ${getFlag(llist.get(7))} axpy ${llist.get(7).getClass.getSimpleName}: ${getFlag(llist.get(7))} is ${(llist.get(7).axpy(llist.get(7), 2.0)).sum()}, and breeze is ${sum(sorted4 + sorted4 * 2)}")

    assert(abs((llist.get(0).axpy(llist.get(0), 2.0)).sum() - sum(sparse1 + sparse1 * 2.0)) < 1.0E-8)
    assert(abs((llist.get(1).axpy(llist.get(1), 2.0)).sum() - sum(sorted1 + sorted1 * 2.0)) < 1.0E-8)
    assert(abs((llist.get(2).axpy(llist.get(2), 2.0f)).sum() - sum(sparse2 + sparse2 * 2.0f)) < 1.0E-2)
    assert(abs((llist.get(3).axpy(llist.get(3), 2.0f)).sum() - sum(sorted2 + sorted2 * 2.0f)) < 1.0E-3)
    assert(abs((llist.get(4).axpy(llist.get(4), 2l)).sum() - sum(sparse3 + sparse3 * 2l)) < 1.0E-8)
    assert(abs((llist.get(5).axpy(llist.get(5), 2l)).sum() - sum(sorted3 + sorted3 * 2l)) < 1.0E-8)
    assert(abs((llist.get(6).axpy(llist.get(6), 2)).sum() - sum(sparse4 + sparse4 * 2)) < 1.0E-8)
    assert(abs((llist.get(7).axpy(llist.get(7), 2)).sum() - sum(sorted4 + sorted4 * 2)) < 1.0E-8)

    val isparse1 = llist.get(0).iaxpy(llist.get(0), 2.0)
    val isorted1 = llist.get(1).iaxpy(llist.get(1), 2.0)
    val isparse2 = llist.get(2).iaxpy(llist.get(2), 2.0f)
    val isorted2 = llist.get(3).iaxpy(llist.get(3), 2.0f)
    val isparse3 = llist.get(4).iaxpy(llist.get(4), 2L)
    val isorted3 = llist.get(5).iaxpy(llist.get(5), 2L)
    val isparse4 = llist.get(6).iaxpy(llist.get(6), 2)
    val isorted4 = llist.get(7).iaxpy(llist.get(7), 2)


    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())
    assert((llist.get(4)).sum() == (isparse3).sum())
    assert((llist.get(5)).sum() == (isorted3).sum())
    assert((llist.get(6)).sum() == (isparse4).sum())
    assert((llist.get(7)).sum() == (isorted4).sum())
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
