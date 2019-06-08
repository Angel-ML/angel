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

import breeze.numerics.abs
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompCompBinaryLongkeyOPTest {
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

  val list = new util.ArrayList[Vector]()

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


    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1_1 = Array(sparse1, sorted1)
    val list1_2 = Array(sorted1, sparse1)
    val comp1_1 = VFactory.compLongDoubleVector(dim * list1_1.length, list1_1)
    val comp1_2 = VFactory.compLongDoubleVector(dim * list1_2.length, list1_2)

    list.add(comp1_1)
    list.add(comp1_2)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2_1 = Array(sparse2, sorted2)
    val list2_2 = Array(sorted2, sparse2)
    val comp2_1 = VFactory.compLongFloatVector(dim * list2_1.length, list2_1)
    val comp2_2 = new CompLongFloatVector(dim * list2_2.length, list2_2)


    list.add(comp2_1)
    list.add(comp2_2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3_1 = Array(sparse3, sorted3)
    val list3_2 = Array(sorted3, sparse3)
    val comp3_1 = new CompLongLongVector(dim * list3_1.length, list3_1)
    val comp3_2 = new CompLongLongVector(dim * list3_2.length, list3_2)

    list.add(comp3_1)
    list.add(comp3_2)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4_1 = Array(sparse4, sorted4)
    val list4_2 = Array(sorted4, sparse4)

    val comp4_1 = new CompLongIntVector(dim * list4_1.length, list4_1)
    val comp4_2 = new CompLongIntVector(dim * list4_2.length, list4_2)

    list.add(comp4_1)
    list.add(comp4_2)
  }
}

class CompCompBinaryLongkeyOPTest {
  val list = CompCompBinaryLongkeyOPTest.list

  val times = 5
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def compVScompAddTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(list.get(0))
      list.get(2).add(list.get(2))
      list.get(4).add(list.get(4))
      list.get(6).add(list.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp longkey add:$cost1")


    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        assert(abs(list.get(i).add(list.get(j)).sum() - (list.get(i).sum() + list.get(j).sum())) < 1.0)
      }
    }
  }

  @Test
  def compVScompSubTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).sub(list.get(0))
      list.get(2).sub(list.get(2))
      list.get(4).sub(list.get(4))
      list.get(6).sub(list.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp longkey sub:$cost1")


    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        assert(abs(list.get(i).sub(list.get(j)).sum() - (list.get(i).sum() - list.get(j).sum())) < 1.0)
      }
    }
  }

  @Test
  def compVScompMulTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).mul(list.get(0))
      list.get(2).mul(list.get(2))
      list.get(4).mul(list.get(4))
      list.get(6).mul(list.get(6))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp longkey mul:$cost1")


    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        list.get(i).mul(list.get(j)).sum()
      }
    }
  }

  @Test
  def compVScompDivTest() {
    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        try{
          list.get(i).div(list.get(j)).sum()
        }catch {
          case e:ArithmeticException =>{
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
  def compVScompAxpyTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).axpy(list.get(0), 2.0)
      list.get(2).axpy(list.get(2), 2.0)
      list.get(4).axpy(list.get(4), 2.0)
      list.get(6).axpy(list.get(6), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp longkey axpy:$cost1")


    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        assert(abs(list.get(i).axpy(list.get(j), 2.0).sum() - (list.get(i).sum() + list.get(j).sum() * 2.0)) < 1.0)
      }
    }
  }
}