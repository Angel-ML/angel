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
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.scalatest.FunSuite

class BLK extends FunSuite {
  val capacity: Int = 1000
  val dim: Int = capacity * 100

  val intrandIndices: Array[Int] = new Array[Int](capacity)
  val longrandIndices: Array[Long] = new Array[Long](capacity)
  val intsortedIndices: Array[Int] = new Array[Int](capacity)
  val longsortedIndices: Array[Long] = new Array[Long](capacity)

  val intrandIndices1: Array[Int] = new Array[Int](capacity)
  val longrandIndices1: Array[Long] = new Array[Long](capacity)
  val intsortedIndices1: Array[Int] = new Array[Int](capacity)
  val longsortedIndices1: Array[Long] = new Array[Long](capacity)

  val intValues: Array[Int] = new Array[Int](capacity)
  val longValues: Array[Long] = new Array[Long](capacity)
  val longValues1: Array[Long] = new Array[Long](capacity)
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

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        intrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    set.clear()
    idx = 0
    while (set.size() < capacity) {
      val t = rand.nextInt(dim)
      if (!set.contains(t)) {
        longrandIndices1(idx) = t
        set.add(t)
        idx += 1
      }
    }

    System.arraycopy(intrandIndices, 0, intsortedIndices, 0, capacity)
    util.Arrays.sort(intsortedIndices)

    System.arraycopy(longrandIndices, 0, longsortedIndices, 0, capacity)
    util.Arrays.sort(longsortedIndices)

    System.arraycopy(intrandIndices1, 0, intsortedIndices1, 0, capacity)
    util.Arrays.sort(intsortedIndices1)

    System.arraycopy(longrandIndices1, 0, longsortedIndices1, 0, capacity)
    util.Arrays.sort(longsortedIndices1)

    doubleValues.indices.foreach { i =>
      doubleValues(i) = rand.nextDouble()
    }

    floatValues.indices.foreach { i =>
      floatValues(i) = rand.nextFloat()
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(100)
    }

    longValues1.indices.foreach { i =>
      longValues1(i) = rand.nextInt(100)
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


  test("div long key test") {
    init()
    println(s"${llist.get(0).div(llist.get(0)).sum()}, ${sum(sparse1 :/ sparse1)}")
    println(s"${llist.get(0).div(llist.get(1)).sum()}, ${sum(sparse1 :/ sorted1)}")
    println(s"${llist.get(1).div(llist.get(0)).sum()}, ${sum(sorted1 :/ sparse1)}")
    println(s"${llist.get(1).div(llist.get(1)).sum()}, ${sum(sorted1 :/ sorted1)}")
    println(s"${llist.get(2).div(llist.get(2)).sum()}, ${sum(sparse2 :/ sparse2)}")
    println(s"${llist.get(2).div(llist.get(3)).sum()}, ${sum(sparse2 :/ sorted2)}")
    println(s"${llist.get(3).div(llist.get(2)).sum()}, ${sum(sorted2 :/ sparse2)}")
    println(s"${llist.get(3).div(llist.get(2)).sum()}, ${sum(sorted2 :/ sorted2)}")


    println(s"${llist.get(0).idiv(llist.get(0)).sum()}")
    println(s"${llist.get(0).idiv(llist.get(1)).sum()}")
    println(s"${llist.get(1).idiv(llist.get(0)).sum()}")
    println(s"${llist.get(1).idiv(llist.get(1)).sum()}")
    println(s"${llist.get(2).idiv(llist.get(2)).sum()}")
    println(s"${llist.get(2).idiv(llist.get(3)).sum()}")
    println(s"${llist.get(3).idiv(llist.get(2)).sum()}")
    println(s"${llist.get(3).idiv(llist.get(3)).sum()}")

    //    (0 until 4).foreach{ i =>
    //      (i+1 until llist.size()).foreach{ j =>
    //        println(s"$i, $j, ${llist.get(i).div(llist.get(j)).sum()}")
    //      }
    //    }
  }

}
