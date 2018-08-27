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
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.Test
import org.scalatest.FunSuite

object UnaryLongKeyTest {
  val capacity: Int = 1000
  val dim: Int = capacity * 100

  val intrandIndices: Array[Int] = new Array[Int](capacity)
  val longrandIndices: Array[Long] = new Array[Long](capacity)
  val intsortedIndices: Array[Int] = new Array[Int](capacity)
  val longsortedIndices: Array[Long] = new Array[Long](capacity)

  val floatValues: Array[Float] = new Array[Float](capacity)
  val doubleValues: Array[Double] = new Array[Double](capacity)

  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)

  val llist = new util.ArrayList[Vector]()

  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0F))


  val times = 500
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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
      floatValues(i) = rand.nextFloat() + 0.01f
    }

    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
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
  }

}

class UnaryLongKeyTest {
  val capacity: Int = UnaryLongKeyTest.capacity
  val dim: Int = UnaryLongKeyTest.dim

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  val llist = UnaryLongKeyTest.llist

  var sparse1 = UnaryLongKeyTest.sparse1
  var sorted1 = UnaryLongKeyTest.sorted1

  var sparse2 = UnaryLongKeyTest.sparse2
  var sorted2 = UnaryLongKeyTest.sorted2


  @Test
  def powTest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(llist.get(0), 2.0)
      Ufuncs.pow(llist.get(2), 2.0f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(sparse1, 2.0)
      pow(sparse2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(llist.get(1), 2.0)
      Ufuncs.pow(llist.get(3), 2.0f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      pow(sorted1, 2.0)
      pow(sorted2, 2.0f)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted pow:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} pow ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${Ufuncs.pow(llist.get(0), 2.0).sum()}, and breeze is ${sum(pow(sparse1, 2.0))}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} pow ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${Ufuncs.pow(llist.get(1), 2.0).sum()}, and breeze is ${sum(pow(sorted1, 2.0))}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} pow ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${Ufuncs.pow(llist.get(2), 2.0f).sum()}, and breeze is ${sum(pow(sparse2, 2.0f))}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} pow ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${Ufuncs.pow(llist.get(3), 2.0f).sum()}, and breeze is ${sum(pow(sorted2, 2.0f))}")


    assert(abs(Ufuncs.pow(llist.get(0), 2.0).sum() - sum(pow(sparse1, 2.0))) < 1.0E-8)
    assert(abs(Ufuncs.pow(llist.get(1), 2.0).sum() - sum(pow(sorted1, 2.0))) < 1.0E-8)
    assert(abs(Ufuncs.pow(llist.get(2), 2.0f).sum() - sum(pow(sparse2, 2.0f))) < 1.0E-3)
    assert(abs(Ufuncs.pow(llist.get(3), 2.0f).sum() - sum(pow(sorted2, 2.0f))) < 1.0E-3)


    val isparse1 = Ufuncs.ipow(llist.get(0), 2.0)
    val isorted1 = Ufuncs.ipow(llist.get(1), 2.0)
    val isparse2 = Ufuncs.ipow(llist.get(2), 2.0f)
    val isorted2 = Ufuncs.ipow(llist.get(3), 2.0f)

    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())

  }

  @Test
  def sqrtTest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(llist.get(0))
      Ufuncs.sqrt(llist.get(2))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sparse1)
      sqrt(sparse2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(llist.get(1))
      Ufuncs.sqrt(llist.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sqrt(sorted1)
      sqrt(sorted2)
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sqrt:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} sqrt ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${Ufuncs.sqrt(llist.get(0)).sum()}, and breeze is ${sum(sqrt(sparse1))}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} sqrt ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${Ufuncs.sqrt(llist.get(1)).sum()}, and breeze is ${sum(sqrt(sorted1))}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} sqrt ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${Ufuncs.sqrt(llist.get(2)).sum()}, and breeze is ${sum(sqrt(sparse2))}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} sqrt ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${Ufuncs.sqrt(llist.get(3)).sum()}, and breeze is ${sum(sqrt(sorted2))}")


    assert(abs(Ufuncs.sqrt(llist.get(0)).sum() - sum(sqrt(sparse1))) < 1.0E-8)
    assert(Ufuncs.sqrt(llist.get(1)).sum() == sum(sqrt(sorted1)))
    assert(abs(Ufuncs.sqrt(llist.get(2)).sum() - sum(sqrt(sparse2))) < 1.0E-3)
    assert(abs(Ufuncs.sqrt(llist.get(3)).sum() - sum(sqrt(sorted2))) < 1.0E-3)

    val isparse1 = Ufuncs.isqrt(llist.get(0))
    val isorted1 = Ufuncs.isqrt(llist.get(1))
    val isparse2 = Ufuncs.isqrt(llist.get(2))
    val isorted2 = Ufuncs.isqrt(llist.get(3))

    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())

  }

  @Test
  def smulTest() {
    //sparse cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(llist.get(0), 0.5)
      Ufuncs.smul(llist.get(2), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :* 0.5
      sparse2 :* 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    //sorted cost
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(llist.get(1), 0.5)
      Ufuncs.smul(llist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :* 0.5
      sorted2 :* 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted smul:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} smul ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${Ufuncs.smul(llist.get(0), 0.5).sum()}, and breeze is ${sum(sparse1 :* 0.5)}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} smul ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${Ufuncs.smul(llist.get(1), 0.5).sum()}, and breeze is ${sum(sorted1 :* 0.5)}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} smul ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${Ufuncs.smul(llist.get(2), 0.5f).sum()}, and breeze is ${sum(sparse2 :* 0.5f)}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} smul ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${Ufuncs.smul(llist.get(3), 0.5f).sum()}, and breeze is ${sum(sorted2 :* 0.5f)}")

    assert(abs(Ufuncs.smul(llist.get(0), 0.5).sum() - sum(sparse1 :* 0.5)) < 1.0E-8)
    assert(Ufuncs.smul(llist.get(1), 0.5).sum() == sum(sorted1 :* 0.5))
    assert(abs(Ufuncs.smul(llist.get(2), 0.5f).sum() - sum(sparse2 :* 0.5f)) < 1.0E-3)
    assert(abs(Ufuncs.smul(llist.get(3), 0.5f).sum() - sum(sorted2 :* 0.5f)) < 1.0E-3)

    val isparse1 = Ufuncs.ismul(llist.get(0), 0.5)
    val isorted1 = Ufuncs.ismul(llist.get(1), 0.5)
    val isparse2 = Ufuncs.ismul(llist.get(2), 0.5f)
    val isorted2 = Ufuncs.ismul(llist.get(3), 0.5f)

    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())

  }

  @Test
  def sdivTest() {

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(llist.get(0), 0.5)
      Ufuncs.sdiv(llist.get(2), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sparse1 :/ 0.5
      sparse2 :/ 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sparse sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")

    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(llist.get(1), 0.5)
      Ufuncs.sdiv(llist.get(3), 0.5f)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1

    start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      sorted1 :/ 0.5
      sorted2 :/ 0.5f
    }
    stop2 = System.currentTimeMillis()
    cost2 = stop2 - start2
    println(s"angel sorted sdiv:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")


    println(s"${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} sdiv ${llist.get(0).getClass.getSimpleName}: ${getFlag(llist.get(0))} is ${Ufuncs.sdiv(llist.get(0), 0.5).sum()}, and breeze is ${sum(sparse1 :/ 0.5)}")
    println(s"${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} sdiv ${llist.get(1).getClass.getSimpleName}: ${getFlag(llist.get(1))} is ${Ufuncs.sdiv(llist.get(1), 0.5).sum()}, and breeze is ${sum(sorted1 :/ 0.5)}")
    println(s"${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} sdiv ${llist.get(2).getClass.getSimpleName}: ${getFlag(llist.get(2))} is ${Ufuncs.sdiv(llist.get(2), 0.5f).sum()}, and breeze is ${sum(sparse2 :/ 0.5f)}")
    println(s"${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} sdiv ${llist.get(3).getClass.getSimpleName}: ${getFlag(llist.get(3))} is ${Ufuncs.sdiv(llist.get(3), 0.5f).sum()}, and breeze is ${sum(sorted2 :/ 0.5f)}")

    assert(abs(Ufuncs.sdiv(llist.get(0), 0.5).sum() - sum(sparse1 :/ 0.5)) < 1.0E-8)
    assert(Ufuncs.sdiv(llist.get(1), 0.5).sum() == sum(sorted1 :/ 0.5))
    assert(abs(Ufuncs.sdiv(llist.get(2), 0.5f).sum() - sum(sparse2 :/ 0.5f)) < 1.0E-2)
    assert(abs(Ufuncs.sdiv(llist.get(3), 0.5f).sum() - sum(sorted2 :/ 0.5f)) < 1.0E-3)

    val isparse1 = Ufuncs.isdiv(llist.get(0), 0.5)
    val isorted1 = Ufuncs.isdiv(llist.get(1), 0.5)
    val isparse2 = Ufuncs.isdiv(llist.get(2), 0.5f)
    val isorted2 = Ufuncs.isdiv(llist.get(3), 0.5f)

    assert((llist.get(0)).sum() == (isparse1).sum())
    assert((llist.get(1)).sum() == (isorted1).sum())
    assert((llist.get(2)).sum() == (isparse2).sum())
    assert((llist.get(3)).sum() == (isorted2).sum())

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
