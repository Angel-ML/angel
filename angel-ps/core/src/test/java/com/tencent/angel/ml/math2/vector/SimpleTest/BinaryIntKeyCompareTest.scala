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

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.Vector
import org.junit.{BeforeClass, Test}

object BinaryIntkeyCompareTest {
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
      doubleValues(i) = 1
    }

    floatValues.indices.foreach { i =>
      floatValues(i) = -1
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(10) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(10) + 1
    }

    densedoubleValues.indices.foreach { i =>
      if (i%2==0){
        densedoubleValues(i) = -1
      }else{
        densedoubleValues(i) = 0
      }

//      rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      if (i%2==0){
        densefloatValues(i) = 0
      }else{
        densefloatValues(i) = -1
      }

//        rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(10) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(10) + 1
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

  }
}

class BinaryIntkeyCompareTest {
  val ilist = BinaryIntkeyCompareTest.ilist

  @Test
  def min(): Unit ={
    println(Ufuncs.min(ilist.get(0),ilist.get(3)).sum(),ilist.get(0).sum(),ilist.get(3).sum())
    println(Ufuncs.min(ilist.get(1),ilist.get(4)).sum(),ilist.get(1).sum(),ilist.get(4).sum())
    println(Ufuncs.min(ilist.get(2),ilist.get(5)).sum(),ilist.get(2).sum(),ilist.get(5).sum())
  }

  @Test
  def max(): Unit ={
    println(Ufuncs.max(ilist.get(0),ilist.get(3)).sum(),ilist.get(0).sum(),ilist.get(3).sum())
    println(Ufuncs.max(ilist.get(1),ilist.get(4)).sum(),ilist.get(1).sum(),ilist.get(4).sum())
    println(Ufuncs.max(ilist.get(2),ilist.get(5)).sum(),ilist.get(2).sum(),ilist.get(5).sum())
  }
}
