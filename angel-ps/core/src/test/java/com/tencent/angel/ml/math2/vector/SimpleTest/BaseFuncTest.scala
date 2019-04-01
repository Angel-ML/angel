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

import breeze.linalg.sum
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.junit.{BeforeClass, Test}

object BaseFuncTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 1000
  val dim: Int = capacity * 1000

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

  val indexValues: Array[Int] = new Array[Int](capacity)
  val longIndexValues: Array[Long] = new Array[Long](capacity)

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

    indexValues.indices.foreach { i =>
      indexValues(i) = rand.nextInt(capacity)
    }

    longIndexValues.indices.foreach { i =>
      longIndexValues(i) = rand.nextInt(dim)
    }
  }
}

class BaseFuncTest {
  val matrixId = BaseFuncTest.matrixId
  val rowId = BaseFuncTest.rowId
  val clock = BaseFuncTest.clock
  val capacity: Int = BaseFuncTest.capacity
  val dim: Int = BaseFuncTest.dim

  val intrandIndices: Array[Int] = BaseFuncTest.intrandIndices
  val longrandIndices: Array[Long] = BaseFuncTest.longrandIndices
  val intsortedIndices: Array[Int] = BaseFuncTest.intsortedIndices
  val longsortedIndices: Array[Long] = BaseFuncTest.longsortedIndices

  val intValues: Array[Int] = BaseFuncTest.intValues
  val longValues: Array[Long] = BaseFuncTest.longValues
  val floatValues: Array[Float] = BaseFuncTest.floatValues
  val doubleValues: Array[Double] = BaseFuncTest.doubleValues

  val denseintValues: Array[Int] = BaseFuncTest.denseintValues
  val denselongValues: Array[Long] = BaseFuncTest.denselongValues
  val densefloatValues: Array[Float] = BaseFuncTest.densefloatValues
  val densedoubleValues: Array[Double] = BaseFuncTest.densedoubleValues

  val indexValues: Array[Int] = BaseFuncTest.indexValues
  val longIndexValues: Array[Long] = BaseFuncTest.longIndexValues

  @Test
  def IntDoubleVectortest() {
    val zero_list = new util.ArrayList[IntDoubleVector]
    val list = new util.ArrayList[IntDoubleVector]

    zero_list.add(VFactory.denseDoubleVector(capacity))
    zero_list.add(VFactory.denseDoubleVector(matrixId, rowId, clock, capacity))
    list.add(VFactory.denseDoubleVector(matrixId, rowId, clock, doubleValues))
    list.add(VFactory.denseDoubleVector(doubleValues))

    zero_list.add(VFactory.sparseDoubleVector(dim))
    zero_list.add(VFactory.sparseDoubleVector(dim, capacity))
    zero_list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseDoubleVector(matrixId, rowId, clock, dim, intrandIndices, doubleValues))
    list.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))

    zero_list.add(VFactory.sortedDoubleVector(dim))
    zero_list.add(VFactory.sortedDoubleVector(dim, capacity))
    zero_list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues))
    list.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    var numzero = 0
    for (v <- doubleValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(s"size:${v.asInstanceOf[Vector].getSize}, ${v.size()}")
      println(v.getType)
      println(s"${sum(v.getStorage.oneLikeDense().getValues)}, ${sum(v.getStorage.oneLikeSparse().getValues)},${sum(v.getStorage.oneLikeSorted().getValues)}")
      println(s"${sum(v.getStorage.oneLikeDense(100).getValues)}, ${sum(v.getStorage.oneLikeSparse(100, 50).getValues)},${sum(v.getStorage.oneLikeSorted(100, 50).getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100).getValues)},${sum(v.getStorage.oneLikeSorted(100).getValues)}")
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }

      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.average == v.average & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }

    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 2.0)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 2.0)
    assert(v3 != zero_list(0).get(0))

    //test setDim()
    val v4 = VFactory.denseDoubleVector(matrixId, rowId, clock, doubleValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def IntFloatVectortest() {
    val zero_list = new util.ArrayList[IntFloatVector]
    val list = new util.ArrayList[IntFloatVector]

    zero_list.add(VFactory.denseFloatVector(capacity))
    zero_list.add(VFactory.denseFloatVector(matrixId, rowId, clock, capacity))
    list.add(VFactory.denseFloatVector(matrixId, rowId, clock, floatValues))
    list.add(VFactory.denseFloatVector(floatValues))

    zero_list.add(VFactory.sparseFloatVector(dim))
    zero_list.add(VFactory.sparseFloatVector(dim, capacity))
    zero_list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseFloatVector(matrixId, rowId, clock, dim, intrandIndices, floatValues))
    list.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))

    zero_list.add(VFactory.sortedFloatVector(dim))
    zero_list.add(VFactory.sortedFloatVector(dim, capacity))
    zero_list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))
    list.add(VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues))

    var numzero = 0
    for (v <- floatValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"${sum(v.getStorage.oneLikeDense().getValues)}, ${sum(v.getStorage.oneLikeSparse().getValues)},${sum(v.getStorage.oneLikeSorted().getValues)}")
      println(s"${sum(v.getStorage.oneLikeDense(100).getValues)}, ${sum(v.getStorage.oneLikeSparse(100, 50).getValues)},${sum(v.getStorage.oneLikeSorted(100, 50).getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100).getValues)},${sum(v.getStorage.oneLikeSorted(100).getValues)}")
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }

      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.average == v.average & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }

    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 2.0f)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 2.0f)
    assert(v3 != zero_list(0).get(0))

    //test setDim()
    val v4 = VFactory.denseFloatVector(matrixId, rowId, clock, floatValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def IntLongVectortest() {
    val zero_list = new util.ArrayList[IntLongVector]
    val list = new util.ArrayList[IntLongVector]

    zero_list.add(VFactory.denseLongVector(capacity))
    zero_list.add(VFactory.denseLongVector(matrixId, rowId, clock, capacity))
    list.add(VFactory.denseLongVector(matrixId, rowId, clock, longValues))
    list.add(VFactory.denseLongVector(longValues))

    zero_list.add(VFactory.sparseLongVector(dim))
    zero_list.add(VFactory.sparseLongVector(dim, capacity))
    zero_list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseLongVector(matrixId, rowId, clock, dim, intrandIndices, longValues))
    list.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))

    zero_list.add(VFactory.sortedLongVector(dim))
    zero_list.add(VFactory.sortedLongVector(dim, capacity))
    zero_list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))
    list.add(VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues))

    var numzero = 0
    for (v <- longValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"${sum(v.getStorage.oneLikeDense().getValues)}, ${sum(v.getStorage.oneLikeSparse().getValues)},${sum(v.getStorage.oneLikeSorted().getValues)}")
      println(s"${sum(v.getStorage.oneLikeDense(100).getValues)}, ${sum(v.getStorage.oneLikeSparse(100, 50).getValues)},${sum(v.getStorage.oneLikeSorted(100, 50).getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100).getValues)},${sum(v.getStorage.oneLikeSorted(100).getValues)}")
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == dim - capacity + numzero)
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 200L)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 200L)
    assert(v3 != zero_list(0).get(0))

    //test setDim()
    val v4 = VFactory.denseLongVector(matrixId, rowId, clock, longValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def IntIntVectortest() {
    val zero_list = new util.ArrayList[IntIntVector]
    val list = new util.ArrayList[IntIntVector]

    zero_list.add(VFactory.denseIntVector(capacity))
    zero_list.add(VFactory.denseIntVector(matrixId, rowId, clock, capacity))
    list.add(VFactory.denseIntVector(matrixId, rowId, clock, intValues))
    list.add(VFactory.denseIntVector(intValues))

    zero_list.add(VFactory.sparseIntVector(dim))
    zero_list.add(VFactory.sparseIntVector(dim, capacity))
    zero_list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseIntVector(matrixId, rowId, clock, dim, intrandIndices, intValues))
    list.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))

    zero_list.add(VFactory.sortedIntVector(dim))
    zero_list.add(VFactory.sortedIntVector(dim, capacity))
    zero_list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(matrixId, rowId, clock, dim, capacity, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))
    list.add(VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues))

    var numzero = 0
    for (v <- intValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"${sum(v.getStorage.oneLikeDense().getValues)}, ${sum(v.getStorage.oneLikeSparse().getValues)},${sum(v.getStorage.oneLikeSorted().getValues)}")
      println(s"${sum(v.getStorage.oneLikeDense(100).getValues)}, ${sum(v.getStorage.oneLikeSparse(100, 50).getValues)},${sum(v.getStorage.oneLikeSorted(100, 50).getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100).getValues)},${sum(v.getStorage.oneLikeSorted(100).getValues)}")
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == dim - capacity + numzero)
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(indexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(indexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 200)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 200)
    assert(v3 != zero_list(0).get(0))

    //test setDim()
    val v4 = VFactory.denseIntVector(matrixId, rowId, clock, intValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def testIntDummyVector() {
    val list = new util.ArrayList[IntDummyVector]

    list.add(VFactory.intDummyVector(dim, intsortedIndices))
    list.add(VFactory.intDummyVector(matrixId, rowId, clock, dim, intsortedIndices))

    val size = intValues.length

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.numZeros == dim - size)
      assert(v.getDim == dim)
      assert(v.size == size)
      assert(v.isDense == false)
      assert(v.isSparse == false)
      assert(v.isSorted == false)
      try {
        println(v.get(0), v.get(intsortedIndices(0)))
        println(v.get(-1))
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          println(e)
        }
      }
      try {
        println(v.hasKey(0), v.hasKey(intsortedIndices(0)))
        println(v.hasKey(-1))
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          println(e)
        }
      }
      v.clear()
    }
    //test setDim()
    val v4 = VFactory.intDummyVector(matrixId, rowId, clock, dim, intValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)

  }

  @Test
  def LongDummyVectortest() {
    val list = new util.ArrayList[LongDummyVector]

    list.add(VFactory.longDummyVector(dim, longsortedIndices))
    list.add(VFactory.longDummyVector(matrixId, rowId, clock, dim, longsortedIndices))

    val size = longValues.length

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.numZeros == dim - size)
      assert(v.getDim == dim)
      assert(v.size == size)
      assert(v.isDense == false)
      assert(v.isSparse == false)
      assert(v.isSorted == false)
      try {
        println(v.get(0), v.get(longsortedIndices(0)))
        println(v.get(-1))
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          println(e)
        }
      }
      try {
        println(v.hasKey(0), v.hasKey(longsortedIndices(0)))
        println(v.hasKey(-1))
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          println(e)
        }
      }
      v.clear()
    }
    //test setDim()
    val v4 = VFactory.longDummyVector(matrixId, rowId, clock, dim, longValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def LongDoubleVectortest() {
    val zero_list = new util.ArrayList[LongDoubleVector]
    val list = new util.ArrayList[LongDoubleVector]

    zero_list.add(VFactory.sparseLongKeyDoubleVector(dim))
    zero_list.add(VFactory.sparseLongKeyDoubleVector(dim, capacity))
    zero_list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, longrandIndices, doubleValues))
    list.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))

    zero_list.add(VFactory.sortedLongKeyDoubleVector(dim))
    zero_list.add(VFactory.sortedLongKeyDoubleVector(dim, capacity))
    zero_list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))
    list.add(VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues))

    var numzero = 0
    for (v <- doubleValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"${sum(v.getStorage.oneLikeSparse().getValues)},${sum(v.getStorage.oneLikeSorted().getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100, 50).getValues)},${sum(v.getStorage.oneLikeSorted(100, 50).getValues)}")
      println(s"${sum(v.getStorage.oneLikeSparse(100).getValues)},${sum(v.getStorage.oneLikeSorted(100).getValues)}")
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 2.0)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 2.0)
    assert(v3 != zero_list(0).get(0))

    //test setDim()
    val v4 = VFactory.sparseLongKeyDoubleVector(matrixId, rowId, clock, dim, longrandIndices, doubleValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def LongFloatVectortest() {
    val zero_list = new util.ArrayList[LongFloatVector]
    val list = new util.ArrayList[LongFloatVector]

    zero_list.add(VFactory.sparseLongKeyFloatVector(dim))
    zero_list.add(VFactory.sparseLongKeyFloatVector(dim, capacity))
    zero_list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, longrandIndices, floatValues))
    list.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))

    zero_list.add(VFactory.sortedLongKeyFloatVector(dim))
    zero_list.add(VFactory.sortedLongKeyFloatVector(dim, capacity))
    zero_list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))
    list.add(VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues))

    var numzero = 0
    for (v <- floatValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 2.0f)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 2.0f)
    assert(v3 != zero_list(0).get(0))

    val v4 = VFactory.sparseLongKeyFloatVector(matrixId, rowId, clock, dim, longrandIndices, floatValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def LongLongVectortest() {
    val zero_list = new util.ArrayList[LongLongVector]
    val list = new util.ArrayList[LongLongVector]

    zero_list.add(VFactory.sparseLongKeyLongVector(dim))
    zero_list.add(VFactory.sparseLongKeyLongVector(dim, capacity))
    zero_list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, longrandIndices, longValues))
    list.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))

    zero_list.add(VFactory.sortedLongKeyLongVector(dim))
    zero_list.add(VFactory.sortedLongKeyLongVector(dim, capacity))
    zero_list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))
    list.add(VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues))

    var numzero = 0
    for (v <- longValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 200L)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 200L)
    assert(v3 != zero_list(0).get(0))

    val v4 = VFactory.sparseLongKeyLongVector(matrixId, rowId, clock, dim, longrandIndices, longValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }

  @Test
  def LongIntVectortest() {
    val zero_list = new util.ArrayList[LongIntVector]
    val list = new util.ArrayList[LongIntVector]

    zero_list.add(VFactory.sparseLongKeyIntVector(dim))
    zero_list.add(VFactory.sparseLongKeyIntVector(dim, capacity))
    zero_list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, longrandIndices, intValues))
    list.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))

    zero_list.add(VFactory.sortedLongKeyIntVector(dim))
    zero_list.add(VFactory.sortedLongKeyIntVector(dim, capacity))
    zero_list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim))
    zero_list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity))
    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(matrixId, rowId, clock, dim, capacity, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))
    list.add(VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues))

    var numzero = 0
    for (v <- intValues) {
      if (v == 0.0) {
        numzero += 1
      }
    }

    import scala.collection.JavaConversions._
    for (v <- list) {
      println(v.getType)
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      assert(v.size == capacity)
      if (v.isDense) {
        assert(v.numZeros == numzero)
        assert(v.getDim == capacity)
      } else {
        assert(v.numZeros == (dim - capacity + numzero))
        assert(v.getDim == dim)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    for (v <- zero_list) {
      println(s"angel result: ${v.numZeros()}, ${v.getDim}, ${v.size()}")
      if (v.isDense) {
        assert(v.numZeros == capacity)
        assert(v.getDim == capacity)
        assert(v.size == capacity)
      } else {
        assert(v.numZeros == dim)
        assert(v.norm == 0.0)
        assert(v.getDim == dim)
        assert(v.size == 0.0)
      }
      val c = v.clone()
      assert(c != v & c.sum() == v.sum() & c.getDim == v.getDim & c.max == v.max & c.min == v.min & c.size == v.size)

      val v1 = v.get(longIndexValues)
      var i = 0
      for (j <- 0 until v1.length) {
        assert(v.get(longIndexValues(i)) == v1(j))
        i += 1
      }
    }
    //test set()
    val v2 = list(0).get(0)
    list(0).set(0, 200)
    assert(v2 != list(0).get(0))

    val v3 = zero_list(0).get(0)
    zero_list(0).set(0, 200)
    assert(v3 != zero_list(0).get(0))

    val v4 = VFactory.sparseLongKeyIntVector(matrixId, rowId, clock, dim, longrandIndices, intValues)
    val sdim = v4.getDim
    v4.setDim(1)
    assert(v4.getDim != sdim)
  }
}
