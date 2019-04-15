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

object CompCompBinaryIntkeyOPTest {
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

    list.add(comp1_1)
    list.add(comp1_2)
    list.add(comp1_3)
    list.add(comp1_4)
    list.add(comp1_5)
    list.add(comp1_6)

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

    list.add(comp2_1)
    list.add(comp2_2)
    list.add(comp2_3)
    list.add(comp2_4)
    list.add(comp2_5)
    list.add(comp2_6)

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

    list.add(comp3_1)
    list.add(comp3_2)
    list.add(comp3_3)
    list.add(comp3_4)
    list.add(comp3_5)
    list.add(comp3_6)

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

    list.add(comp4_1)
    list.add(comp4_2)
    list.add(comp4_3)
    list.add(comp4_4)
    list.add(comp4_5)
    list.add(comp4_6)
  }
}

class CompCompBinaryIntkeyOPTest {
  val list = CompCompBinaryIntkeyOPTest.list

  val times = 5
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def compVScompAddTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      list.get(0).add(list.get(0))
      list.get(6).add(list.get(6))
      list.get(12).add(list.get(12))
      list.get(18).add(list.get(18))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp intkey add:$cost1")


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
      list.get(6).sub(list.get(6))
      list.get(12).sub(list.get(12))
      list.get(18).sub(list.get(18))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp intkey sub:$cost1")


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
      list.get(6).mul(list.get(6))
      list.get(12).mul(list.get(12))
      list.get(18).mul(list.get(18))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp intkey mul:$cost1")


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
      list.get(6).axpy(list.get(6), 2.0)
      list.get(12).axpy(list.get(12), 2.0)
      list.get(18).axpy(list.get(18), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp vs comp intkey axpy:$cost1")


    (0 until 1).foreach { i =>
      (i until 3).foreach { j =>
        assert(abs(list.get(i).axpy(list.get(j), 2.0).sum() - (list.get(i).sum() + list.get(j).sum() * 2.0)) < 1.0)
      }
    }
  }
}