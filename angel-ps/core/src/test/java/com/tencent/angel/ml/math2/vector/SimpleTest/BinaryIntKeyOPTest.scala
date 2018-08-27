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
import breeze.linalg.{DenseVector, HashVector, SparseVector, sum}
import breeze.numerics._
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.SimpleTest.BinaryIntKeyOPTest._
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}

object BinaryIntKeyOPTest {
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

  val doubledummyValues: Array[Double] = new Array[Double](capacity)
  val floatdummyValues: Array[Float] = new Array[Float](capacity)
  val longdummyValues: Array[Long] = new Array[Long](capacity)
  val intdummyValues: Array[Int] = new Array[Int](capacity)

  val denseintValues: Array[Int] = new Array[Int](dim)
  val denselongValues: Array[Long] = new Array[Long](dim)
  val densefloatValues: Array[Float] = new Array[Float](dim)
  val densedoubleValues: Array[Double] = new Array[Double](dim)


  val ilist = new util.ArrayList[Vector]()
  val llist = new util.ArrayList[Vector]()

  var sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
  var dense1 = DenseVector[Double](densedoubleValues)
  var sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

  var dense2 = DenseVector[Float](densefloatValues)
  var sparse2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
  var sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, default = 0.0f))

  var dense3 = DenseVector[Long](denselongValues)
  var sparse3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
  var sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, default = 0l))

  var dense4 = DenseVector[Int](denseintValues)
  var sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
  var sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

  var intdummy1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubledummyValues, capacity, dim, default = 0))
  var intdummy2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatdummyValues, capacity, dim, default = 0))
  var intdummy3 = new SparseVector[Long](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))
  var intdummy4 = new SparseVector[Int](new SparseArray(intsortedIndices, intdummyValues, capacity, dim, default = 0))

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
      longValues(i) = rand.nextInt(10) + 1L
    }

    intValues.indices.foreach { i =>
      intValues(i) = rand.nextInt(10) + 1
    }

    doubledummyValues.indices.foreach { i =>
      doubledummyValues(i) = 1.0
    }

    floatdummyValues.indices.foreach { i =>
      floatdummyValues(i) = 1.0f
    }

    longdummyValues.indices.foreach { i =>
      longdummyValues(i) = 1L
    }

    intdummyValues.indices.foreach { i =>
      intdummyValues(i) = 1
    }

    densedoubleValues.indices.foreach { i =>
      densedoubleValues(i) = rand.nextDouble()
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat()
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

    sparse1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => sparse1(i) = v }
    dense1 = DenseVector[Double](densedoubleValues)
    sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, default = 0.0))

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
    ilist.add(VFactory.intDummyVector(dim, intsortedIndices))

    dense4 = DenseVector[Int](denseintValues)
    sparse4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => sparse4(i) = v }
    sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, default = 0))

    intdummy1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubledummyValues, capacity, dim, default = 0))
    intdummy2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatdummyValues, capacity, dim, default = 0))
    intdummy3 = new SparseVector[Long](new SparseArray(intsortedIndices, longdummyValues, capacity, dim, default = 0))
    intdummy4 = new SparseVector[Int](new SparseArray(intsortedIndices, intdummyValues, capacity, dim, default = 0))
  }
}

class BinaryIntKeyOPTest {
  val ilist = BinaryIntKeyOPTest.ilist

  var sparse1 = BinaryIntKeyOPTest.sparse1
  var dense1 = BinaryIntKeyOPTest.dense1
  var sorted1 = BinaryIntKeyOPTest.sorted1

  var dense2 = BinaryIntKeyOPTest.dense2
  var sparse2 = BinaryIntKeyOPTest.sparse2
  var sorted2 = BinaryIntKeyOPTest.sorted2

  var dense3 = BinaryIntKeyOPTest.dense3
  var sparse3 = BinaryIntKeyOPTest.sparse3
  var sorted3 = BinaryIntKeyOPTest.sorted3

  var dense4 = BinaryIntKeyOPTest.dense4
  var sparse4 = BinaryIntKeyOPTest.sparse4
  var sorted4 = BinaryIntKeyOPTest.sorted4

  var intdummy1 = BinaryIntKeyOPTest.intdummy1
  var intdummy2 = BinaryIntKeyOPTest.intdummy2
  var intdummy3 = BinaryIntKeyOPTest.intdummy3
  var intdummy4 = BinaryIntKeyOPTest.intdummy4

  @Test
  def Addtest() {
    println("angel add test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} add ${ilist.get(j).getClass.getSimpleName}: ${getFlag(ilist.get(j))} is ${(ilist.get(i).add(ilist.get(j))).sum()}")
          if (getFlag(ilist.get(j)) != "dummy") {
            assert(abs((ilist.get(i).add(ilist.get(j))).sum() - (ilist.get(i).sum() + ilist.get(j).sum())) < 1.0E-3)
          } else {
            assert(abs((ilist.get(i).add(ilist.get(j))).sum() - (ilist.get(i).sum() + sum(intdummy1))) < 1.0E-3)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }

    assert((ilist.get(0).add(ilist.get(0))).sum() == sum(dense1 + dense1))
    assert(abs((ilist.get(1).add(ilist.get(1))).sum() - sum(sparse1 + sparse1)) < 1.0E-8)
    assert((ilist.get(2).add(ilist.get(2))).sum() == sum(sorted1 + sorted1))
    assert(abs(ilist.get(3).add(ilist.get(3)).sum() - sum(dense2 + dense2)) < 1.0)
    assert(abs((ilist.get(4).add(ilist.get(4))).sum() - sum(sparse2 + sparse2)) < 1.0E-3)
    assert(abs((ilist.get(5).add(ilist.get(5))).sum() - sum(sorted2 + sorted2)) < 1.0E-3)
    assert((ilist.get(6).add(ilist.get(6))).sum() == sum(dense3 + dense3))
    assert((ilist.get(7).add(ilist.get(7))).sum() == sum(sparse3 + sparse3))
    assert((ilist.get(8).add(ilist.get(8))).sum() == sum(sorted3 + sorted3))
    assert((ilist.get(9).add(ilist.get(9))).sum() == sum(dense4 + dense4))
    assert((ilist.get(10).add(ilist.get(10))).sum() == sum(sparse4 + sparse4))
    assert((ilist.get(11).add(ilist.get(11))).sum() == sum(sorted4 + sorted4))


  }

  @Test
  def Subtest() {
    println("angel sub test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).getClass.getSimpleName}: ${getFlag(ilist.get(i))} sub ${ilist.get(j).getClass.getSimpleName}: ${getFlag(ilist.get(j))} is ${(ilist.get(i).sub(ilist.get(j))).sum()}")
          if (getFlag(ilist.get(i)) != "dummy") {
            assert(abs((ilist.get(i).sub(ilist.get(j))).sum() - (ilist.get(i).sum() - ilist.get(j).sum())) < 1.0E-3)
          } else {
            assert(abs((ilist.get(i).sub(ilist.get(j))).sum() - (ilist.get(i).sum() - sum(intdummy1))) < 1.0E-3)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }

    assert((ilist.get(0).sub(ilist.get(0))).sum() == sum(dense1 - dense1))
    assert((ilist.get(1).sub(ilist.get(1))).sum() == sum(sparse1 - sparse1))
    assert((ilist.get(2).sub(ilist.get(2))).sum() == sum(sorted1 - sorted1))
    assert((ilist.get(3).sub(ilist.get(3))).sum() == sum(dense2 - dense2))
    assert((ilist.get(4).sub(ilist.get(4))).sum() == sum(sparse2 - sparse2))
    assert((ilist.get(5).sub(ilist.get(5))).sum() == sum(sorted2 - sorted2))
    assert((ilist.get(6).sub(ilist.get(6))).sum() == sum(dense3 - dense3))
    assert((ilist.get(7).sub(ilist.get(7))).sum() == sum(sparse3 - sparse3))
    assert((ilist.get(8).sub(ilist.get(8))).sum() == sum(sorted3 - sorted3))
    assert((ilist.get(9).sub(ilist.get(9))).sum() == sum(dense4 - dense4))
    assert((ilist.get(10).sub(ilist.get(10))).sum() == sum(sparse4 - sparse4))
    assert((ilist.get(11).sub(ilist.get(11))).sum() == sum(sorted4 - sorted4))

    println("angel isub test--")
    val idense1 = ilist.get(0).isub(ilist.get(0))
    val isparse1 = ilist.get(1).isub(ilist.get(1))
    val isorted1 = ilist.get(2).isub(ilist.get(2))
    val idense2 = ilist.get(3).isub(ilist.get(3))
    val isparse2 = ilist.get(4).isub(ilist.get(4))
    val isorted2 = ilist.get(5).isub(ilist.get(5))
    val idense3 = ilist.get(6).isub(ilist.get(6))
    val isparse3 = ilist.get(7).isub(ilist.get(7))
    val isorted3 = ilist.get(8).isub(ilist.get(8))
    val idense4 = ilist.get(9).isub(ilist.get(9))
    val isparse4 = ilist.get(10).isub(ilist.get(10))
    val isorted4 = ilist.get(11).isub(ilist.get(11))


    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())
    assert((ilist.get(6)).sum() == (idense3).sum())
    assert((ilist.get(7)).sum() == (isparse3).sum())
    assert((ilist.get(8)).sum() == (isorted3).sum())
    assert((ilist.get(9)).sum() == (idense4).sum())
    assert((ilist.get(10)).sum() == (isparse4).sum())
    assert((ilist.get(11)).sum() == (isorted4).sum())

  }

  @Test
  def Multest() {
    println("angel mul test--")
    assert(abs((ilist.get(0).mul(ilist.get(0))).sum() - sum(dense1 :* dense1)) < 1.0E-8)
    assert(abs((ilist.get(1).mul(ilist.get(1))).sum() - sum(sparse1 :* sparse1)) < 1.0E-8)
    assert(abs((ilist.get(2).mul(ilist.get(2))).sum() - sum(sorted1 :* sorted1)) < 1.0E-8)
    assert(abs(ilist.get(3).mul(ilist.get(3)).sum() - sum(dense2 :* dense2)) < 1.0)
    assert(abs((ilist.get(4).mul(ilist.get(4))).sum() - sum(sparse2 :* sparse2)) < 1.0E-3)
    assert(abs((ilist.get(5).mul(ilist.get(5))).sum() - sum(sorted2 :* sorted2)) < 1.0E-3)
    assert(abs((ilist.get(6).mul(ilist.get(6))).sum() - sum(dense3 :* dense3)) < 1.0E-8)
    assert(abs((ilist.get(7).mul(ilist.get(7))).sum() - sum(sparse3 :* sparse3)) < 1.0E-8)
    assert(abs((ilist.get(8).mul(ilist.get(8))).sum() - sum(sorted3 :* sorted3)) < 1.0E-8)
    assert(abs((ilist.get(9).mul(ilist.get(9))).sum() - sum(dense4 :* dense4)) < 1.0E-8)
    assert(abs((ilist.get(10).mul(ilist.get(10))).sum() - sum(sparse4 :* sparse4)) < 1.0E-8)
    assert(abs((ilist.get(11).mul(ilist.get(11))).sum() - sum(sorted4 :* sorted4)) < 1.0E-8)

    assert(abs((ilist.get(0).mul(ilist.get(12))).sum() - sum(dense1 :* intdummy1)) < 1.0E-8)
    assert(abs((ilist.get(1).mul(ilist.get(12))).sum() - sum(sparse1 :* intdummy1)) < 1.0E-8)
    assert(abs((ilist.get(2).mul(ilist.get(12))).sum() - sum(sorted1 :* intdummy1)) < 1.0E-8)
    assert(abs(ilist.get(3).mul(ilist.get(12)).sum() - sum(dense2 :* intdummy2)) < 1.0)
    assert(abs((ilist.get(4).mul(ilist.get(12))).sum() - sum(sparse2 :* intdummy2)) < 1.0E-3)
    assert(abs((ilist.get(5).mul(ilist.get(12))).sum() - sum(sorted2 :* intdummy2)) < 1.0E-3)
    assert(abs((ilist.get(6).mul(ilist.get(12))).sum() - sum(dense3 :* intdummy3)) < 1.0E-8)
    assert(abs((ilist.get(7).mul(ilist.get(12))).sum() - sum(sparse3 :* intdummy3)) < 1.0E-8)
    assert(abs((ilist.get(8).mul(ilist.get(12))).sum() - sum(sorted3 :* intdummy3)) < 1.0E-8)
    assert(abs((ilist.get(9).mul(ilist.get(12))).sum() - sum(dense4 :* intdummy4)) < 1.0E-8)
    assert(abs((ilist.get(10).mul(ilist.get(12))).sum() - sum(sparse4 :* intdummy4)) < 1.0E-8)
    assert(abs((ilist.get(11).mul(ilist.get(12))).sum() - sum(sorted4 :* intdummy4)) < 1.0E-8)

    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).mul(ilist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
    println("angel imul test--")
    val idense1 = ilist.get(0).imul(ilist.get(0))
    val isparse1 = ilist.get(1).imul(ilist.get(1))
    val isorted1 = ilist.get(2).imul(ilist.get(2))
    val idense2 = ilist.get(3).imul(ilist.get(3))
    val isparse2 = ilist.get(4).imul(ilist.get(4))
    val isorted2 = ilist.get(5).imul(ilist.get(5))
    val idense3 = ilist.get(6).imul(ilist.get(6))
    val isparse3 = ilist.get(7).imul(ilist.get(7))
    val isorted3 = ilist.get(8).imul(ilist.get(8))
    val idense4 = ilist.get(9).imul(ilist.get(9))
    val isparse4 = ilist.get(10).imul(ilist.get(10))
    val isorted4 = ilist.get(11).imul(ilist.get(11))


    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())
    assert((ilist.get(6)).sum() == (idense3).sum())
    assert((ilist.get(7)).sum() == (isparse3).sum())
    assert((ilist.get(8)).sum() == (isorted3).sum())
    assert((ilist.get(9)).sum() == (idense4).sum())
    assert((ilist.get(10)).sum() == (isparse4).sum())
    assert((ilist.get(11)).sum() == (isorted4).sum())

  }

  @Test
  def Divtest() {
    init()
    println("angel div test--")

    //    assert((ilist.get(0).div(ilist.get(0))).sum() == sum(dense1 :/ dense1))
    //    assert((ilist.get(1).div(ilist.get(1))).sum() == sum(sparse1 :/ sparse1))
    //    assert((ilist.get(2).mul(ilist.get(2))).sum() == sum(sorted1 :/ sorted1))
    //    assert((ilist.get(3).div(ilist.get(3))).sum() == sum(dense2 :/ dense2))
    //    assert((ilist.get(4).mul(ilist.get(4))).sum() == sum(sparse2 :/ sparse2))
    //    assert((ilist.get(5).div(ilist.get(5))).sum() == sum(sorted2 :/ sorted2))
    //    assert((ilist.get(6).div(ilist.get(6))).sum() == sum(dense3 :/ dense3))
    //    assert((ilist.get(7).div(ilist.get(7))).sum() == sum(sparse3 :/ sparse3))
    //    assert((ilist.get(8).div(ilist.get(8))).sum() == sum(sorted3 :/ sorted3))
    //    assert((ilist.get(9).div(ilist.get(9))).sum() == sum(dense4 :/ dense4))
    //    assert((ilist.get(10).div(ilist.get(10))).sum() == sum(sparse4 :/ sparse4))
    //    assert((ilist.get(11).div(ilist.get(11))).sum() == sum(sorted4 :/ sorted4))

    println(ilist.get(0).div(ilist.get(0)).sum(), sum(dense1 :/ dense1))
    println(ilist.get(1).div(ilist.get(1)).sum(), sum(sparse1 :/ sparse1))
    println(ilist.get(2).div(ilist.get(2)).sum(), sum(sorted1 :/ sorted1))
    println(ilist.get(3).div(ilist.get(3)).sum(), sum(dense2 :/ dense2))
    println(ilist.get(4).div(ilist.get(4)).sum(), sum(sparse2 :/ sparse2))
    println(ilist.get(5).div(ilist.get(5)).sum(), sum(sorted2 :/ sorted2))
    println(ilist.get(6).div(ilist.get(6)).sum(), sum(dense3 :/ dense3))
    //    println((ilist.get(7).div(ilist.get(7))).sum() )
    //    println((ilist.get(8).div(ilist.get(8))).sum() )
    println(ilist.get(9).div(ilist.get(9)).sum(), sum(dense4 :/ dense4))
    //    println((ilist.get(10).div(ilist.get(10))).sum() )
    //    println((ilist.get(11).div(ilist.get(11))).sum() )


    //    (0 until ilist.size()).foreach{ i =>
    //      (0 until ilist.size()).foreach{ j =>
    //        try{
    //          println(s"${ilist.get(i).div(ilist.get(j)).sum()}")
    //        }catch{
    //          case e: AngelException =>{
    //            println(e)
    //          }
    //        }
    //      }
    //    }

    val idense1 = ilist.get(0).idiv(ilist.get(0))
    val isparse1 = ilist.get(1).idiv(ilist.get(1))
    val isorted1 = ilist.get(2).idiv(ilist.get(2))
    val idense2 = ilist.get(3).idiv(ilist.get(3))
    val isparse2 = ilist.get(4).idiv(ilist.get(4))
    val isorted2 = ilist.get(5).idiv(ilist.get(5))
    val idense3 = ilist.get(6).idiv(ilist.get(6))
    //    val isparse3 = ilist.get(7).idiv(ilist.get(7))
    //    val isorted3 = ilist.get(8).idiv(ilist.get(8))
    val idense4 = ilist.get(9).idiv(ilist.get(9))
    //    val isparse4 = ilist.get(10).idiv(ilist.get(10))
    //    val isorted4 = ilist.get(11).idiv(ilist.get(11))
    //
    println(idense1.sum(), isparse1.sum(), isorted1.sum(), idense1.sum(), isparse1.sum(), isorted1.sum(), idense3.sum(), idense4.sum())
    //    assert((ilist.get(0)).sum() == (idense1).sum())
    //    assert((ilist.get(1)).sum() == (isparse1).sum())
    //    assert((ilist.get(2)).sum() == (isorted1).sum())
    //    assert((ilist.get(3)).sum() == (idense2).sum())
    //    assert((ilist.get(4)).sum()== (isparse2).sum())
    //    assert((ilist.get(5)).sum()== (isorted2).sum())
    //    assert((ilist.get(6)).sum() == (idense3).sum())
    //    assert((ilist.get(7)).sum() == (isparse3).sum())
    //    assert((ilist.get(8)).sum() == (isorted3).sum())
    //    assert((ilist.get(9)).sum() == (idense4).sum())
    //    assert((ilist.get(10)).sum()== (isparse4).sum())
    //    assert((ilist.get(11)).sum()== (isorted4).sum())
  }

  @Test
  def Axpytest() {
    init()
    println("angel axpy test--")
    (0 until ilist.size()).foreach { i =>
      (0 until ilist.size()).foreach { j =>
        try {
          println(s"${ilist.get(i).axpy(ilist.get(j), 2.0).sum()}")
          if (getFlag(ilist.get(i)) != "dummy") {
            assert(abs((ilist.get(i).axpy(ilist.get(j), 2.0)).sum() - (ilist.get(i).sum() + ilist.get(j).sum() * 2)) < 1.0E-3)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} axpy ${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} is ${(ilist.get(0).axpy(ilist.get(0), 2.0)).sum()}, and breeze is ${sum(dense1 + dense1 * 2.0)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} axpy ${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} is ${(ilist.get(1).axpy(ilist.get(1), 2.0)).sum()}, and breeze is ${sum(sparse1 + sparse1 * 2.0)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} axpy ${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} is ${(ilist.get(2).axpy(ilist.get(2), 2.0)).sum()}, and breeze is ${sum(sorted1 + sorted1 * 2.0)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} axpy ${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} is ${(ilist.get(3).axpy(ilist.get(3), 2.0f)).sum()}, and breeze is ${sum(dense2 + dense2 * 2.0f)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} axpy ${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} is ${(ilist.get(4).axpy(ilist.get(4), 2.0f)).sum()}, and breeze is ${sum(sparse2 + sparse2 * 2.0f)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} axpy ${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} is ${(ilist.get(5).axpy(ilist.get(5), 2.0f)).sum()}, and breeze is ${sum(sorted2 + sorted2 * 2.0f)}")
    println(s"${ilist.get(6).getClass.getSimpleName}: ${getFlag(ilist.get(6))} axpy ${ilist.get(6).getClass.getSimpleName}: ${getFlag(ilist.get(6))} is ${(ilist.get(6).axpy(ilist.get(6), 2l)).sum()}, and breeze is ${sum(dense3 + dense3 * 2L)}")
    println(s"${ilist.get(7).getClass.getSimpleName}: ${getFlag(ilist.get(7))} axpy ${ilist.get(7).getClass.getSimpleName}: ${getFlag(ilist.get(7))} is ${(ilist.get(7).axpy(ilist.get(7), 2l)).sum()}, and breeze is ${sum(sparse3 + sparse3 * 2L)}")
    println(s"${ilist.get(8).getClass.getSimpleName}: ${getFlag(ilist.get(8))} axpy ${ilist.get(8).getClass.getSimpleName}: ${getFlag(ilist.get(8))} is ${(ilist.get(8).axpy(ilist.get(8), 2l)).sum()}, and breeze is ${sum(sorted3 + sorted3 * 2L)}")
    println(s"${ilist.get(9).getClass.getSimpleName}: ${getFlag(ilist.get(9))} axpy ${ilist.get(9).getClass.getSimpleName}: ${getFlag(ilist.get(9))} is ${(ilist.get(9).axpy(ilist.get(9), 2)).sum()}, and breeze is ${sum(dense4 + dense4 * 2)}")
    println(s"${ilist.get(10).getClass.getSimpleName}: ${getFlag(ilist.get(10))} axpy ${ilist.get(10).getClass.getSimpleName}: ${getFlag(ilist.get(10))} is ${(ilist.get(10).axpy(ilist.get(10), 2)).sum()}, and breeze is ${sum(sparse4 + sparse4 * 2)}")
    println(s"${ilist.get(11).getClass.getSimpleName}: ${getFlag(ilist.get(11))} axpy ${ilist.get(11).getClass.getSimpleName}: ${getFlag(ilist.get(11))} is ${(ilist.get(11).axpy(ilist.get(11), 2)).sum()}, and breeze is ${sum(sorted4 + sorted4 * 2)}")

    assert(abs(ilist.get(0).axpy(ilist.get(0), 2.0).sum() - sum(dense1 + dense1 * 2.0)) < 1.0E-8)
    assert(abs((ilist.get(1).axpy(ilist.get(1), 2.0)).sum() - sum(sparse1 + sparse1 * 2.0)) < 1.0E-8)
    assert(abs((ilist.get(2).axpy(ilist.get(2), 2.0)).sum() - sum(sorted1 + sorted1 * 2.0)) < 1.0E-8)
    assert(abs((ilist.get(3).axpy(ilist.get(3), 2.0f)).sum() - sum(dense2 + dense2 * 2.0f)) < 1.0)
    assert(abs((ilist.get(4).axpy(ilist.get(4), 2.0f)).sum() - sum(sparse2 + sparse2 * 2.0f)) < 1.0E-3)
    assert(abs((ilist.get(5).axpy(ilist.get(5), 2.0f)).sum() - sum(sorted2 + sorted2 * 2.0f)) < 1.0E-3)
    assert(abs((ilist.get(6).axpy(ilist.get(6), 2l)).sum() - sum(dense3 + dense3 * 2L)) < 1.0E-8)
    assert(abs((ilist.get(7).axpy(ilist.get(7), 2l)).sum() - sum(sparse3 + sparse3 * 2L)) < 1.0E-8)
    assert(abs((ilist.get(8).axpy(ilist.get(8), 2l)).sum() - sum(sorted3 + sorted3 * 2L)) < 1.0E-8)
    assert(abs((ilist.get(9).axpy(ilist.get(9), 2)).sum() - sum(dense4 + dense4 * 2)) < 1.0E-8)
    assert(abs((ilist.get(10).axpy(ilist.get(10), 2)).sum() - sum(sparse4 + sparse4 * 2)) < 1.0E-8)
    assert(abs((ilist.get(11).axpy(ilist.get(11), 2)).sum() - sum(sorted4 + sorted4 * 2)) < 1.0E-8)

    println("angel iaxpy test--")
    val idense1 = ilist.get(0).iaxpy(ilist.get(0), 2.0)
    val isparse1 = ilist.get(1).iaxpy(ilist.get(1), 2.0)
    val isorted1 = ilist.get(2).iaxpy(ilist.get(2), 2.0)
    val idense2 = ilist.get(3).iaxpy(ilist.get(3), 2.0f)
    val isparse2 = ilist.get(4).iaxpy(ilist.get(4), 2.0f)
    val isorted2 = ilist.get(5).iaxpy(ilist.get(5), 2.0f)
    val idense3 = ilist.get(6).iaxpy(ilist.get(6), 2L)
    val isparse3 = ilist.get(7).iaxpy(ilist.get(7), 2L)
    val isorted3 = ilist.get(8).iaxpy(ilist.get(8), 2L)
    val idense4 = ilist.get(9).iaxpy(ilist.get(9), 2)
    val isparse4 = ilist.get(10).iaxpy(ilist.get(10), 2)
    val isorted4 = ilist.get(11).iaxpy(ilist.get(11), 2)


    assert((ilist.get(0)).sum() == (idense1).sum())
    assert((ilist.get(1)).sum() == (isparse1).sum())
    assert((ilist.get(2)).sum() == (isorted1).sum())
    assert((ilist.get(3)).sum() == (idense2).sum())
    assert((ilist.get(4)).sum() == (isparse2).sum())
    assert((ilist.get(5)).sum() == (isorted2).sum())
    assert((ilist.get(6)).sum() == (idense3).sum())
    assert((ilist.get(7)).sum() == (isparse3).sum())
    assert((ilist.get(8)).sum() == (isorted3).sum())
    assert((ilist.get(9)).sum() == (idense4).sum())
    assert((ilist.get(10)).sum() == (isparse4).sum())
    assert((ilist.get(11)).sum() == (isorted4).sum())

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
