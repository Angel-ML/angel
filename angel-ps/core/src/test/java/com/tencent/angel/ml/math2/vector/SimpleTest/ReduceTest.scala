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
import breeze.linalg.{DenseVector, HashVector, SparseVector, argmax, argmin, max, min, sum}
import breeze.numerics._
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object ReduceTest {
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
  }
}

class ReduceTest {
  val matrixId = ReduceTest.matrixId
  val rowId = ReduceTest.rowId
  val clock = ReduceTest.clock
  val capacity: Int = ReduceTest.capacity
  val dim: Int = ReduceTest.dim

  val intrandIndices: Array[Int] = ReduceTest.intrandIndices
  val longrandIndices: Array[Long] = ReduceTest.longrandIndices
  val intsortedIndices: Array[Int] = ReduceTest.intsortedIndices
  val longsortedIndices: Array[Long] = ReduceTest.longsortedIndices

  val intValues: Array[Int] = ReduceTest.intValues
  val longValues: Array[Long] = ReduceTest.longValues
  val floatValues: Array[Float] = ReduceTest.floatValues
  val doubleValues: Array[Double] = ReduceTest.doubleValues

  val denseintValues: Array[Int] = ReduceTest.denseintValues
  val denselongValues: Array[Long] = ReduceTest.denselongValues
  val densefloatValues: Array[Float] = ReduceTest.densefloatValues
  val densedoubleValues: Array[Double] = ReduceTest.densedoubleValues

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def reduceIntDoubleVectortest() {
    val list = new util.ArrayList[IntDoubleVector]

    list.add(VFactory.denseDoubleVector(matrixId, rowId, clock, densedoubleValues))
    list.add(VFactory.denseDoubleVector(densedoubleValues))
    val dense1 = new DenseVector[Double](densedoubleValues)

    list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, intrandIndices, doubleValues))
    list.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    val sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))
    val sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0))

    println(s"angel dense max:${list.get(0).max()}, breeze:${max(dense1)}")
    println(s"angel sparse max:${list.get(2).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(4).max()}, breeze:${max(sorted1)}")
    println(s"angel dense max:${list.get(0).argmax()}, breeze:${argmax(dense1)}")
    println(s"angel sparse max:${list.get(2).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(4).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel dense min:${list.get(0).min()}, breeze:${min(dense1)}")
    println(s"angel sparse min:${list.get(2).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(4).min()}, breeze:${min(sorted1)}")
    println(s"angel dense min:${list.get(0).argmin()}, breeze:${argmin(dense1)}")
    println(s"angel sparse min:${list.get(2).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(4).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).sum()==sum(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).sum()-sum(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).sum()==sum(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).max()==max(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).min()==min(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).min()-min(sorted1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).average()-breeze.stats.mean(dense1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).average()-breeze.stats.mean(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).average()-breeze.stats.mean(sorted1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(dense1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).std()-  breeze.stats.stddev(dense1))<1.0E-5)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).std()-  breeze.stats.stddev(sparse1))<1.0E-6)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(4).std()-  breeze.stats.stddev(sorted1))<1.0E-6)
  }

  @Test
  def reduceIntFloatVectortest() {
    val list = new util.ArrayList[IntFloatVector]

    list.add(VFactory.denseFloatVector(matrixId, rowId, clock, densefloatValues))
    list.add(VFactory.denseFloatVector(floatValues))
    val dense1 = DenseVector[Float](densefloatValues)

    list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim, intrandIndices, floatValues))
    list.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    val sparse1 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues))
    val sorted1 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

    println(s"angel dense max:${list.get(0).max()}, breeze:${max(dense1)}")
    println(s"angel sparse max:${list.get(2).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(4).max()}, breeze:${max(sorted1)}")
    println(s"angel dense max:${list.get(0).argmax()}, breeze:${argmax(dense1)}")
    println(s"angel sparse max:${list.get(2).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(4).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel dense min:${list.get(0).min()}, breeze:${min(dense1)}")
    println(s"angel sparse min:${list.get(2).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(4).min()}, breeze:${min(sorted1)}")
    println(s"angel dense min:${list.get(0).argmin()}, breeze:${argmin(dense1)}")
    println(s"angel sparse min:${list.get(2).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(4).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).sum()-sum(dense1))<1.0)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).sum()-sum(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).sum()-sum(sorted1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).max()==max(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).min()==min(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).min()-min(sorted1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).average()-breeze.stats.mean(dense1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).average()-breeze.stats.mean(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).average()-breeze.stats.mean(sorted1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(dense1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).std()-  breeze.stats.stddev(dense1))<1.0E-5)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).std()-  breeze.stats.stddev(sparse1))<1.0E-6)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(4).std()-  breeze.stats.stddev(sorted1))<1.0E-6)
  }

  @Test
  def reduceIntLongVectortest() {
    val list = new util.ArrayList[IntLongVector]

    list.add(VFactory.denseLongVector(matrixId, rowId, clock, denselongValues))
    list.add(VFactory.denseLongVector(longValues))
    val dense1 = DenseVector[Long](denselongValues)

    list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim, intrandIndices, longValues))
    list.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    val sparse1 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues))
    val sorted1 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

    println(s"angel dense max:${list.get(0).max()}, breeze:${max(dense1)}")
    println(s"angel sparse max:${list.get(2).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(4).max()}, breeze:${max(sorted1)}")
    println(s"angel dense max:${list.get(0).argmax()}, breeze:${argmax(dense1)}")
    println(s"angel sparse max:${list.get(2).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(4).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel dense min:${list.get(0).min()}, breeze:${min(dense1)}")
    println(s"angel sparse min:${list.get(2).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(4).min()}, breeze:${min(sorted1)}")
    println(s"angel dense min:${list.get(0).argmin()}, breeze:${argmin(dense1)}")
    println(s"angel sparse min:${list.get(2).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(4).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).sum()==sum(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).sum()-sum(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).sum()==sum(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).max()==max(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).min()==min(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).min()-min(sorted1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel dense avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(dense1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel dense std:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse std:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted std:$cost1, breeze:-")
  }

  @Test
  def reduceIntIntVectortest() {
    val list = new util.ArrayList[IntIntVector]

    list.add(VFactory.denseIntVector(matrixId, rowId, clock, denseintValues))
    list.add(VFactory.denseIntVector(intValues))
    val dense1 = DenseVector(denseintValues)

    list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim, intrandIndices, intValues))
    list.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    val sparse1 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues))
    val sorted1 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    println(s"angel dense max:${list.get(0).max()}, breeze:${max(dense1)}")
    println(s"angel sparse max:${list.get(2).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(4).max()}, breeze:${max(sorted1)}")
    println(s"angel dense max:${list.get(0).argmax()}, breeze:${argmax(dense1)}")
    println(s"angel sparse max:${list.get(2).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(4).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel dense min:${list.get(0).min()}, breeze:${min(dense1)}")
    println(s"angel sparse min:${list.get(2).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(4).min()}, breeze:${min(sorted1)}")
    println(s"angel dense min:${list.get(0).argmin()}, breeze:${argmin(dense1)}")
    println(s"angel sparse min:${list.get(2).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(4).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).sum()==sum(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).sum()-sum(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).sum()==sum(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(4).max()==max(sorted1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).min()==min(dense1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).min()-min(sorted1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel dense avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted avg:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(dense1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel dense norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(dense1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(4).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel dense std:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse std:$cost1, breeze:-")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(4).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted std:$cost1, breeze:-")

  }

  @Test
  def reduceIntDummyVectortest() {
    val list = new util.ArrayList[IntDummyVector]

    list.add(VFactory.intDummyVector(dim, intsortedIndices))
    list.add(VFactory.intDummyVector(matrixId, rowId, clock, dim, intsortedIndices))

    val sum1 = intValues.length
    val avg = sum1 * 1.0 / dim
    val norm = Math.sqrt(intValues.length)
    val std = Math.sqrt(avg - avg * avg)
    val size = intValues.length

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(s"angel result:${v.sum()}, ${v.std()}, ${v.average()}, ${v.numZeros()}, ${v.norm()}, ${v.getDim}, ${v.size}")
      println(s"breeze result:${sum1}, ${std}, ${avg}, ${norm}, ${dim}")
      assert(abs(v.sum - sum1) == 0)
      assert(abs(v.std - std) < 1.0E-8)
      assert(abs(v.average - avg) < 1.0E-8)
      assert(v.numZeros == dim - size)
      assert(abs(v.norm - norm) == 0)
      assert(v.getDim == dim)
      assert(v.size == size)
    }
  }

  @Test
  def reduceLongDummyVectortest() {
    val list = new util.ArrayList[LongDummyVector]

    list.add(VFactory.longDummyVector(dim, longsortedIndices))
    list.add(VFactory.longDummyVector(matrixId, rowId, clock, dim, longsortedIndices))

    val sum1 = longValues.length
    val avg = sum1 * 1.0 / dim
    val norm = Math.sqrt(longValues.length)
    val std = Math.sqrt(avg - avg * avg)
    val size = longValues.length

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(s"angel result:${v.sum()}, ${v.std()}, ${v.average()}, ${v.numZeros()}, ${v.norm()}, ${v.getDim}, ${v.size}")
      println(s"breeze result:${sum1}, ${std}, ${avg}, ${norm}, ${dim}")
      assert(abs(v.sum - sum1) == 0)
      assert(abs(v.std - std) < 1.0E-8)
      assert(abs(v.average - avg) < 1.0E-8)
      assert(v.numZeros == dim - size)
      assert(abs(v.norm - norm) == 0)
      assert(v.getDim == dim)
      assert(v.size == size)
    }
  }

  @Test
  def reduceLongDoubleVectortest() {
    val list = new util.ArrayList[LongDoubleVector]

    list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, longrandIndices, doubleValues))
    list.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    val sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues))
    val sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0))


    println(s"angel sparse max:${list.get(0).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(2).max()}, breeze:${max(sorted1)}")
    println(s"angel sparse max:${list.get(0).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(2).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel sparse min:${list.get(0).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(2).min()}, breeze:${min(sorted1)}")
    println(s"angel sparse min:${list.get(0).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(2).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).sum()-sum(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).sum()==sum(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sorted1))<1.0E-2)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).average()-breeze.stats.mean(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).average()-breeze.stats.mean(sorted1))<1.0E-3)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).std()-  breeze.stats.stddev(sparse1))<1.0E-6)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).std()-  breeze.stats.stddev(sorted1))<1.0E-6)
  }

  @Test
  def reduceLongFloatVectortest() {
    val list = new util.ArrayList[LongFloatVector]

    list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, longrandIndices, floatValues))
    list.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    val sparse1 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues))
    val sorted1 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0))

    println(s"angel sparse max:${list.get(0).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(2).max()}, breeze:${max(sorted1)}")
    println(s"angel sparse max:${list.get(0).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(2).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel sparse min:${list.get(0).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(2).min()}, breeze:${min(sorted1)}")
    println(s"angel sparse min:${list.get(0).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(2).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).sum()-sum(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).sum()-sum(sorted1))<1.0E-3)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sorted1))<1.0E-2)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).average()-breeze.stats.mean(sparse1))<1.0E-3)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.mean(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted avg:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).average()-breeze.stats.mean(sorted1))<1.0E-3)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).std()-  breeze.stats.stddev(sparse1))<1.0E-6)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.stats.stddev(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted std:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(2).std()-  breeze.stats.stddev(sorted1))<1.0E-6)
  }

  @Test
  def reduceLongLongVectortest() {
    val list = new util.ArrayList[LongLongVector]

    list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, longrandIndices, longValues))
    list.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    val sparse1 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues))
    val sorted1 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0))

    println(s"angel sparse max:${list.get(0).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(2).max()}, breeze:${max(sorted1)}")
    println(s"angel sparse max:${list.get(0).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(2).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel sparse min:${list.get(0).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(2).min()}, breeze:${min(sorted1)}")
    println(s"angel sparse min:${list.get(0).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(2).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).sum()-sum(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).sum()==sum(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max()==max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max()==max(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).min()-min(sparse1))<1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min()-min(sorted1))<1.0E-2)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse avg:$cost1")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted avg:$cost1")
    //
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm()- breeze.linalg.norm(sparse1))<1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).norm()- breeze.linalg.norm(sorted1))<1.0E-8)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse std:$cost1")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach{ _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted std:$cost1")
  }

  @Test
  def reduceLongIntVectortest() {
    val list = new util.ArrayList[LongIntVector]

    list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, longrandIndices, intValues))
    list.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    val sparse1 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse1(i) = v }

    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues))
    val sorted1 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    println(s"angel sparse max:${list.get(0).max()}, breeze:${max(sparse1)}")
    println(s"angel sorted max:${list.get(2).max()}, breeze:${max(sorted1)}")
    println(s"angel sparse max:${list.get(0).argmax()}, breeze:${argmax(sparse1)}")
    println(s"angel sorted max:${list.get(2).argmax()}, breeze:${argmax(sorted1)}")

    println(s"angel sparse min:${list.get(0).min()}, breeze:${min(sparse1)}")
    println(s"angel sorted min:${list.get(2).min()}, breeze:${min(sorted1)}")
    println(s"angel sparse min:${list.get(0).argmin()}, breeze:${argmin(sparse1)}")
    println(s"angel sorted min:${list.get(2).argmin()}, breeze:${argmin(sorted1)}")

    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      sum(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).sum() - sum(sparse1)) < 1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).sum()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      sum(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted sum:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).sum() == sum(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      max(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(0).max() == max(sparse1))
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).max()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      max(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted max:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(list.get(2).max() == max(sorted1))
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      min(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(0).min() - min(sparse1)) < 1.0E-2)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).min()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      min(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted min:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).min() - min(sorted1)) < 1.0E-2)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse avg:$cost1")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).average()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted avg:$cost1")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      breeze.linalg.norm(sparse1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sparse norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //    assert(abs(list.get(0).norm() - breeze.linalg.norm(sparse1)) < 1.0E-8)
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).norm()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    start2 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      breeze.linalg.norm(sorted1)
    //    }
    //    stop2 = System.currentTimeMillis()
    //    cost2 = stop2 - start2
    //    println(s"angel sorted norm:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")
    //
    //    assert(abs(list.get(2).norm() - breeze.linalg.norm(sorted1)) < 1.0E-8)
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(0).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sparse std:$cost1")
    //
    //
    //    start1 = System.currentTimeMillis()
    //    (0 to times).foreach { _ =>
    //      list.get(2).std()
    //    }
    //    stop1 = System.currentTimeMillis()
    //    cost1 = stop1 - start1
    //    println(s"angel sorted std:$cost1")
  }
}
