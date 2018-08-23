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

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{CompIntDoubleVector, CompIntFloatVector, CompIntIntVector, CompIntLongVector, CompLongDoubleVector, CompLongFloatVector, CompLongIntVector, CompLongLongVector, Vector}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object CompBaseFuncTest {
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

class CompBaseFuncTest {
  val matrixId = CompBaseFuncTest.matrixId
  val rowId = CompBaseFuncTest.rowId
  val clock = CompBaseFuncTest.clock
  val capacity: Int = CompBaseFuncTest.capacity
  val dim: Int = CompBaseFuncTest.dim

  val intrandIndices: Array[Int] = CompBaseFuncTest.intrandIndices
  val longrandIndices: Array[Long] = CompBaseFuncTest.longrandIndices
  val intsortedIndices: Array[Int] = CompBaseFuncTest.intsortedIndices
  val longsortedIndices: Array[Long] = CompBaseFuncTest.longsortedIndices

  val intValues: Array[Int] = CompBaseFuncTest.intValues
  val longValues: Array[Long] = CompBaseFuncTest.longValues
  val floatValues: Array[Float] = CompBaseFuncTest.floatValues
  val doubleValues: Array[Double] = CompBaseFuncTest.doubleValues

  val denseintValues: Array[Int] = CompBaseFuncTest.denseintValues
  val denselongValues: Array[Long] = CompBaseFuncTest.denselongValues
  val densefloatValues: Array[Float] = CompBaseFuncTest.densefloatValues
  val densedoubleValues: Array[Double] = CompBaseFuncTest.densedoubleValues


  @Test
  def CompIntDoubleVectorTest() {
    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = VFactory.compIntDoubleVector(dim * list1.length, list1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = VFactory.compIntFloatVector(dim * list2.length, list2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = VFactory.compIntLongVector(dim * list3.length, list3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = VFactory.compIntIntVector(dim * list4.length, list4)

    println(comp1.asInstanceOf[Vector].getType, comp2.getType, comp3.getType, comp4.getType)
    println(s"${comp1.asInstanceOf[Vector].getSize},${comp1.size()}")
    assert(comp1.getDim == dim * list1.length)
    assert(comp1.getNumPartitions == list1.length)

    //setPartitions
    comp1.getPartitions()
    comp1.setPartitions(Array(sparse1, dense1, sorted1))

    assert(comp1.isCompatable(new CompIntDoubleVector(dim * 3, Array(dense1, sparse1, sorted1))) == true)
    assert(comp1.isCompatable(new CompIntDoubleVector(dim * 2, Array(dense1, sparse1))) == false)
    assert(comp1.isCompatable(new CompIntFloatVector(dim * 3, Array(dense2, sparse2, sorted2))) == true)
    assert(comp1.isCompatable(new CompIntLongVector(dim * 3, Array(dense3, sparse3, sorted3))) == true)
    assert(comp1.isCompatable(new CompIntIntVector(dim * 3, Array(dense4, sparse4, sorted4))) == true)

    comp1.get(16000)
    comp1.set(1600, 10)

    val c = comp1.clone()
    assert(c != comp1)

    assert(comp1.getTypeIndex == 4)
    comp1.clear()
  }

  @Test
  def CompIntFloatVectorTest() {
    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = VFactory.compIntDoubleVector(dim * list1.length, list1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = VFactory.compIntFloatVector(dim * list2.length, list2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = VFactory.compIntLongVector(dim * list3.length, list3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = VFactory.compIntIntVector(dim * list4.length, list4)

    assert(comp2.getDim == dim * list2.length)
    assert(comp2.getNumPartitions == list2.length)

    //setPartitions
    comp2.getPartitions()
    comp2.setPartitions(Array(sparse2, dense2, sorted2))

    assert(comp2.isCompatable(new CompIntDoubleVector(dim * 3, Array(dense1, sparse1, sorted1))) == false)
    assert(comp2.isCompatable(new CompIntFloatVector(dim * 2, Array(dense2, sparse2))) == false)
    assert(comp2.isCompatable(new CompIntFloatVector(dim * 3, Array(dense2, sparse2, sorted2))) == true)
    assert(comp2.isCompatable(new CompIntLongVector(dim * 3, Array(dense3, sparse3, sorted3))) == true)
    assert(comp2.isCompatable(new CompIntIntVector(dim * 3, Array(dense4, sparse4, sorted4))) == true)

    comp2.get(16000)
    comp2.set(1600, 10)

    val c = comp2.clone()
    assert(c != comp2)

    assert(comp2.getTypeIndex == 3)
    comp2.clear()
  }

  @Test
  def CompIntLongVectorTest() {
    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = VFactory.compIntDoubleVector(dim * list1.length, list1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = VFactory.compIntFloatVector(dim * list2.length, list2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = VFactory.compIntLongVector(dim * list3.length, list3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = VFactory.compIntIntVector(dim * list4.length, list4)


    assert(comp3.getDim == dim * list3.length)
    assert(comp3.getNumPartitions == list3.length)

    //setPartitions
    comp3.getPartitions()
    comp3.setPartitions(Array(sparse3, dense3, sorted3))

    assert(comp3.isCompatable(new CompIntDoubleVector(dim * 3, Array(dense1, sparse1, sorted1))) == false)
    assert(comp3.isCompatable(new CompIntFloatVector(dim * 3, Array(dense2, sparse2, sorted2))) == false)
    assert(comp3.isCompatable(new CompIntLongVector(dim * 3, Array(dense3, sparse3, sorted3))) == true)
    assert(comp3.isCompatable(new CompIntLongVector(dim * 2, Array(dense3, sparse3))) == false)
    assert(comp3.isCompatable(new CompIntIntVector(dim * 3, Array(dense4, sparse4, sorted4))) == true)

    comp3.get(16000)
    comp3.set(1600, 10)

    val c = comp3.clone()
    assert(c != comp3)

    assert(comp3.getTypeIndex == 2)
    comp3.clear()
  }

  @Test
  def CompIntIntVectorTest() {
    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = VFactory.compIntDoubleVector(dim * list1.length, list1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = VFactory.compIntFloatVector(dim * list2.length, list2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = VFactory.compIntLongVector(dim * list3.length, list3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = VFactory.compIntIntVector(dim * list4.length, list4)


    assert(comp4.getDim == dim * list4.length)
    assert(comp4.getNumPartitions == list4.length)

    //setPartitions
    comp4.getPartitions()
    comp4.setPartitions(Array(sparse4, dense4, sorted4))

    assert(comp4.isCompatable(new CompIntDoubleVector(dim * 3, Array(dense1, sparse1, sorted1))) == false)
    assert(comp4.isCompatable(new CompIntFloatVector(dim * 3, Array(dense2, sparse2, sorted2))) == false)
    assert(comp4.isCompatable(new CompIntLongVector(dim * 3, Array(dense3, sparse3, sorted3))) == false)
    assert(comp4.isCompatable(new CompIntIntVector(dim * 2, Array(dense4, sparse4))) == false)
    assert(comp4.isCompatable(new CompIntIntVector(dim * 3, Array(dense4, sparse4, sorted4))) == true)

    comp4.get(16000)
    comp4.set(1600, 10)

    val c = comp4.clone()
    assert(c != comp4)

    assert(comp4.getTypeIndex == 1)
    comp4.clear()
  }

  @Test
  def CompLongDoubleVectorTest() {
    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1 = Array(sparse1, sorted1)
    val comp1 = VFactory.compLongDoubleVector(dim * list1.length, list1)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2 = Array(sparse2, sorted2)
    val comp2 = VFactory.compLongFloatVector(dim * list2.length, list2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3 = Array(sparse3, sorted3)
    val comp3 = VFactory.compLongLongVector(dim * list3.length, list3)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4 = Array(sparse4, sorted4)
    val comp4 = VFactory.compLongIntVector(dim * list4.length, list4)

    println(comp1.getType, comp2.getType, comp3.getType, comp4.getType)
    assert(comp1.getDim == dim * list1.length)
    assert(comp1.getNumPartitions == list1.length)

    //setPartitions
    comp1.getPartitions()
    comp1.setPartitions(Array(sorted1, sparse1))

    assert(comp1.isCompatable(new CompLongDoubleVector(dim * 2, Array(sparse1, sorted1))) == true)
    assert(comp1.isCompatable(new CompLongDoubleVector(dim * 3, Array(sorted1, sorted1, sparse1))) == false)
    assert(comp1.isCompatable(new CompLongFloatVector(dim * 2, Array(sparse2, sorted2))) == true)
    assert(comp1.isCompatable(new CompLongLongVector(dim * 2, Array(sparse3, sorted3))) == true)
    assert(comp1.isCompatable(new CompLongIntVector(dim * 2, Array(sparse4, sorted4))) == true)

    comp1.get(16000)
    comp1.set(1600, 10)

    val c = comp1.clone()
    assert(c != comp1)

    assert(comp1.getTypeIndex == 4)
    comp1.clear()
  }

  @Test
  def CompLongFloatVectorTest() {
    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1 = Array(sparse1, sorted1)
    val comp1 = VFactory.compLongDoubleVector(dim * list1.length, list1)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2 = Array(sparse2, sorted2)
    val comp2 = VFactory.compLongFloatVector(dim * list2.length, list2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3 = Array(sparse3, sorted3)
    val comp3 = VFactory.compLongLongVector(dim * list3.length, list3)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4 = Array(sparse4, sorted4)
    val comp4 = VFactory.compLongIntVector(dim * list4.length, list4)


    assert(comp2.getDim == dim * list2.length)
    assert(comp2.getNumPartitions == list2.length)

    //setPartitions
    comp2.getPartitions()
    comp2.setPartitions(Array(sorted2, sparse2))

    assert(comp2.isCompatable(new CompLongDoubleVector(dim * 2, Array(sparse1, sorted1))) == false)
    assert(comp2.isCompatable(new CompLongFloatVector(dim * 3, Array(sorted2, sorted2, sparse2))) == false)
    assert(comp2.isCompatable(new CompLongFloatVector(dim * 2, Array(sparse2, sorted2))) == true)
    assert(comp2.isCompatable(new CompLongLongVector(dim * 2, Array(sparse3, sorted3))) == true)
    assert(comp2.isCompatable(new CompLongIntVector(dim * 2, Array(sparse4, sorted4))) == true)

    comp2.get(16000)
    comp2.set(1600, 10)

    val c = comp2.clone()
    assert(c != comp2)

    assert(comp2.getTypeIndex == 3)
    comp2.clear()
  }

  @Test
  def CompLongLongVectorTest() {
    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1 = Array(sparse1, sorted1)
    val comp1 = VFactory.compLongDoubleVector(dim * list1.length, list1)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2 = Array(sparse2, sorted2)
    val comp2 = VFactory.compLongFloatVector(dim * list2.length, list2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3 = Array(sparse3, sorted3)
    val comp3 = VFactory.compLongLongVector(dim * list3.length, list3)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4 = Array(sparse4, sorted4)
    val comp4 = VFactory.compLongIntVector(dim * list4.length, list4)


    assert(comp3.getDim == dim * list3.length)
    assert(comp3.getNumPartitions == list3.length)

    //setPartitions
    comp3.getPartitions()
    comp3.setPartitions(Array(sorted3, sparse3))

    assert(comp3.isCompatable(new CompLongDoubleVector(dim * 2, Array(sparse1, sorted1))) == false)
    assert(comp3.isCompatable(new CompLongLongVector(dim * 3, Array(sorted3, sorted3, sparse3))) == false)
    assert(comp3.isCompatable(new CompLongFloatVector(dim * 2, Array(sparse2, sorted2))) == false)
    assert(comp3.isCompatable(new CompLongLongVector(dim * 2, Array(sparse3, sorted3))) == true)
    assert(comp3.isCompatable(new CompLongIntVector(dim * 2, Array(sparse4, sorted4))) == true)

    comp3.get(16000)
    comp3.set(1600, 10)

    val c = comp3.clone()
    assert(c != comp3)

    assert(comp3.getTypeIndex == 2)
    comp3.clear()
  }

  @Test
  def CompLongIntVectorTest() {
    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list1 = Array(sparse1, sorted1)
    val comp1 = VFactory.compLongDoubleVector(dim * list1.length, list1)

    val sparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list2 = Array(sparse2, sorted2)
    val comp2 = VFactory.compLongFloatVector(dim * list2.length, list2)

    val sparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list3 = Array(sparse3, sorted3)
    val comp3 = VFactory.compLongLongVector(dim * list3.length, list3)

    val sparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list4 = Array(sparse4, sorted4)
    val comp4 = VFactory.compLongIntVector(dim * list4.length, list4)


    assert(comp4.getDim == dim * list4.length)
    assert(comp4.getNumPartitions == list4.length)

    //setPartitions
    comp4.getPartitions()
    comp4.setPartitions(Array(sorted4, sparse4))

    assert(comp4.isCompatable(new CompLongDoubleVector(dim * 2, Array(sparse1, sorted1))) == false)
    assert(comp4.isCompatable(new CompLongIntVector(dim * 3, Array(sorted4, sorted4, sparse4))) == false)
    assert(comp4.isCompatable(new CompLongFloatVector(dim * 2, Array(sparse2, sorted2))) == false)
    assert(comp4.isCompatable(new CompLongLongVector(dim * 2, Array(sparse3, sorted3))) == false)
    assert(comp4.isCompatable(new CompLongIntVector(dim * 2, Array(sparse4, sorted4))) == true)

    comp4.get(16000)
    comp4.set(1600, 10)

    val c = comp4.clone()
    assert(c != comp4)

    assert(comp4.getTypeIndex == 1)
    comp4.clear()
  }
}
