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
import breeze.linalg.{DenseVector, HashVector, SparseVector}
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}


object UneryCoverageTest {
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
  val slist = new util.ArrayList[Vector]()
  val sllist = new util.ArrayList[Vector]()

  val times = 500
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  var dense1 = DenseVector[Double](densedoubleValues)
  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

  var dense2 = DenseVector[Float](densefloatValues)
  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var dense3 = DenseVector[Long](denselongValues)
  var sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var dense4 = DenseVector[Int](denseintValues)
  var sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

  var lsparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var lsorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

  var lsparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var lsorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var lsparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var lsorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var lsparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var lsorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))


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
      floatValues(i) = rand.nextFloat() + 0.01f
    }

    longValues.indices.foreach { i =>
      longValues(i) = rand.nextInt(100) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(100) + 1
    }


    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(100) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(100) + 1
    }

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    dense1 = DenseVector[Double](densedoubleValues)
    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))

    ilist.add(VFactory.denseFloatVector(densefloatValues))
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))

    dense2 = DenseVector[Float](densefloatValues)
    sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => sparse2(i) = v }
    sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

    ilist.add(VFactory.denseLongVector(denselongValues))
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))

    dense3 = DenseVector[Long](denselongValues)
    sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => sparse3(i) = v }
    sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

    ilist.add(VFactory.denseIntVector(denseintValues))
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))

    dense4 = DenseVector[Int](denseintValues)
    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))
    lsparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => lsparse1(i) = v }
    lsorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))
    lsparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => lsparse2(i) = v }
    lsorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0F))

    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))
    lsparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => lsparse3(i) = v }
    lsorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0L))

    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))
    lsparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => lsparse4(i) = v }
    lsorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))


    slist.add(VFactory.denseDoubleVector(densedoubleValues))
    slist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    slist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))

    slist.add(VFactory.denseFloatVector(densefloatValues))
    slist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    slist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))

    slist.add(VFactory.denseLongVector(denselongValues))
    slist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    slist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))

    slist.add(VFactory.denseIntVector(denseintValues))
    slist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    slist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))


    sllist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    sllist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))

    sllist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    sllist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))

    sllist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    sllist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))

    sllist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    sllist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))
  }
}

class UneryCoverageTest {
  val ilist = UneryCoverageTest.ilist
  val llist = UneryCoverageTest.llist
  val slist = UneryCoverageTest.slist
  val sllist = UneryCoverageTest.sllist

  @Test
  def expTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.exp(ilist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def logTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.log(ilist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def log1pTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.log1p(ilist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def sigmoidTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        TransFuncs.sigmoid(ilist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def softthresholdTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.softthreshold(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def saddTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.sadd(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def ssubTest() {
    //dense cost
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.ssub(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def powTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.pow(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      try {
        Ufuncs.pow(llist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def sqrtTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.sqrt(ilist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      try {
        Ufuncs.sqrt(llist.get(i))
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def smulTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.smul(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      try {
        Ufuncs.smul(llist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def sdivTest() {
    (0 until ilist.size()).foreach { i =>
      try {
        Ufuncs.sdiv(ilist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
    (0 until llist.size()).foreach { i =>
      try {
        Ufuncs.sdiv(llist.get(i), 2.0)
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def iexpTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.iexp(slist.get(i)).sum() == slist.get(i).sum())
    }
  }

  @Test
  def ilogTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.ilog(slist.get(i)).sum() == slist.get(i).sum())
    }
  }

  @Test
  def ilog1pTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.ilog1p(slist.get(i)).sum() == slist.get(i).sum())
    }
  }

  @Test
  def isigmoidTest() {
    (0 until slist.size()).foreach { i =>
      assert(TransFuncs.isigmoid(slist.get(i)).sum() == slist.get(i).sum())
    }
  }

  @Test
  def isothresholdTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.isoftthreshold(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }
  }

  @Test
  def isaddTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.isadd(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }
  }

  @Test
  def issubTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.issub(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }
  }

  @Test
  def ipowTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.ipow(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }
    (0 until sllist.size()).foreach { i =>
      assert(Ufuncs.ipow(sllist.get(i), 2.0).sum() == sllist.get(i).sum())
    }
  }

  @Test
  def isqrtTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.isqrt(slist.get(i)).sum() == slist.get(i).sum())
    }
    (0 until sllist.size()).foreach { i =>
      assert(Ufuncs.isqrt(sllist.get(i)).sum() == sllist.get(i).sum())
    }
  }

  @Test
  def ismulTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.ismul(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }
    (0 until sllist.size()).foreach { i =>
      assert(Ufuncs.ismul(sllist.get(i), 2.0).sum() == sllist.get(i).sum())
    }
  }

  @Test
  def isdivTest() {
    (0 until slist.size()).foreach { i =>
      assert(Ufuncs.isdiv(slist.get(i), 2.0).sum() == slist.get(i).sum())
    }

    (0 until sllist.size()).foreach { i =>
      assert(Ufuncs.isdiv(sllist.get(i), 2.0).sum() == sllist.get(i).sum())
    }
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
