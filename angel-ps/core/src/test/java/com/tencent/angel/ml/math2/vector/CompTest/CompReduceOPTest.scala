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
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object CompReduceOPTest {
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

  val times = 5000
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

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

class CompReduceOPTest {
  val matrixId = CompReduceOPTest.matrixId
  val rowId = CompReduceOPTest.rowId
  val clock = CompReduceOPTest.clock
  val capacity: Int = CompReduceOPTest.capacity
  val dim: Int = CompReduceOPTest.dim

  val intrandIndices: Array[Int] = CompReduceOPTest.intrandIndices
  val longrandIndices: Array[Long] = CompReduceOPTest.longrandIndices
  val intsortedIndices: Array[Int] = CompReduceOPTest.intsortedIndices
  val longsortedIndices: Array[Long] = CompReduceOPTest.longsortedIndices

  val intValues: Array[Int] = CompReduceOPTest.intValues
  val longValues: Array[Long] = CompReduceOPTest.longValues
  val floatValues: Array[Float] = CompReduceOPTest.floatValues
  val doubleValues: Array[Double] = CompReduceOPTest.doubleValues

  val denseintValues: Array[Int] = CompReduceOPTest.denseintValues
  val denselongValues: Array[Long] = CompReduceOPTest.denselongValues
  val densefloatValues: Array[Float] = CompReduceOPTest.densefloatValues
  val densedoubleValues: Array[Double] = CompReduceOPTest.densedoubleValues

  @Test
  def CompIntDoubleVectorTest() {
    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list = Array(dense1, sparse1, sorted1)
    val comp = new CompIntDoubleVector(dim * list.length, list)


    val sum1 = dense1.sum() + sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = dense1.max()

    if (maxValue < sparse1.max()) {
      maxValue = sparse1.max()
    } else if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = dense1.min()
    if (minValue > sparse1.min()) {
      minValue = sparse1.min()
    } else if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(dense1.norm(), 2) + math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)

    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)
    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompIntFloatVectorTest() {
    val dense1 = VFactory.denseFloatVector(densefloatValues)
    val sparse1 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted1 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list = Array(dense1, sparse1, sorted1)
    val comp = new CompIntFloatVector(dim * list.length, list)


    val sum1 = dense1.sum() + sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = dense1.max()

    if (maxValue < sparse1.max()) {
      maxValue = sparse1.max()
    } else if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = dense1.min()
    if (minValue > sparse1.min()) {
      minValue = sparse1.min()
    } else if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(dense1.norm(), 2) + math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)

    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompIntLongVectorTest() {
    val dense1 = VFactory.denseLongVector(denselongValues)
    val sparse1 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted1 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list = Array(dense1, sparse1, sorted1)
    val comp = new CompIntLongVector(dim * list.length, list)


    val sum1 = dense1.sum() + sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = dense1.max()

    if (maxValue < sparse1.max()) {
      maxValue = sparse1.max()
    } else if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = dense1.min()
    if (minValue > sparse1.min()) {
      minValue = sparse1.min()
    } else if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(dense1.norm(), 2) + math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)


    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompIntIntVectorTest() {
    val dense1 = VFactory.denseIntVector(denseintValues)
    val sparse1 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted1 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list = Array(dense1, sparse1, sorted1)
    val comp = new CompIntIntVector(dim * list.length, list)


    val sum1 = dense1.sum() + sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = dense1.max()

    if (maxValue < sparse1.max()) {
      maxValue = sparse1.max()
    } else if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = dense1.min()
    if (minValue > sparse1.min()) {
      minValue = sparse1.min()
    } else if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(dense1.norm(), 2) + math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)


    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompLOngDoubkeVectorTest() {
    val sparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val sorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val list = Array(sparse1, sorted1)
    val comp = new CompLongDoubleVector(dim * list.length, list)


    val sum1 = sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = sparse1.max()

    if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = sparse1.min()
    if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)


    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompLongFloatVectorTest() {
    val sparse1 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val sorted1 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val list = Array(sparse1, sorted1)
    val comp = new CompLongFloatVector(dim * list.length, list)


    val sum1 = sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = sparse1.max()

    if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = sparse1.min()
    if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)


    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompLongLongVectorTest() {
    val sparse1 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val sorted1 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val list = Array(sparse1, sorted1)
    val comp = new CompLongLongVector(dim * list.length, list)


    val sum1 = sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = sparse1.max()

    if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = sparse1.min()
    if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)

    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }

  @Test
  def CompLongIntVectorTest() {
    val sparse1 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val sorted1 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val list = Array(sparse1, sorted1)
    val comp = new CompLongIntVector(dim * list.length, list)


    val sum1 = sparse1.sum() + sorted1.sum()
    val avg = sum1 / comp.getDim
    var maxValue = sparse1.max()

    if (maxValue < sorted1.max()) {
      maxValue = sorted1.max()
    }
    var minValue = sparse1.min()
    if (minValue > sorted1.min()) {
      minValue = sorted1.min()
    }

    var sum2 = math.pow(sparse1.norm(), 2) + math.pow(sorted1.norm(), 2)
    val norm = math.sqrt(sum2)
    val std = math.sqrt(sum2 / comp.getDim - avg * avg)

    assert(comp.sum() == sum1)
    assert(comp.average() == avg)
    assert(comp.max() == maxValue)
    assert(comp.min() == minValue)
    assert(comp.std() == std)
    assert(comp.norm() == norm)

    println(s"dim: ${comp.getDim}, numpartitions: ${comp.getNumPartitions}")
    println(s"sum:${comp.sum()},average:${comp.average()},max:${comp.max()},min:${comp.min()},std:${comp.std()},norm:${comp.norm()}, size:${comp.size()}, numzeros:${comp.numZeros()},claer:${comp.clear()}")
    println(s"sum:${sum1},average:${avg},max:${maxValue},min:${minValue},std:${std},norm:${norm}")

  }
}