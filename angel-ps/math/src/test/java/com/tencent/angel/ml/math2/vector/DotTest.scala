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


package com.tencent.angel.ml.math2.vector

import com.tencent.angel.ml.math2.VFactory
import breeze.linalg._
import org.junit.Test
import java.util

import breeze.collection.mutable.{OpenAddressHashArray, SparseArray}

// angel:28237, breeze:34695, ratio:1.2287070156178064
class DotTest {
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

  @Test def testall() {
    init()
    dotIntKeyVector()
    println("\n\n")
    dotLongKeyVector()
    println()
  }

  def dotIntKeyVector(): Unit = {
    val ilist = new util.ArrayList[Vector]()
    val blist = new util.ArrayList[Any]()

    ilist.add(VFactory.denseDoubleVector(densedoubleValues))
    val dense1 = DenseVector[Double](densedoubleValues)
    blist.add(dense1)
    ilist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    val hash1 = new HashVector[Double](new OpenAddressHashArray[Double](dim))
    intrandIndices.zip(doubleValues).foreach { case (i, v) => hash1(i) = v }
    blist.add(hash1)
    ilist.add(VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues))
    val sorted1 = new SparseVector[Double](new SparseArray(intsortedIndices, doubleValues, capacity, dim, 0.0))
    blist.add(sorted1)

    ilist.add(VFactory.denseFloatVector(densefloatValues))
    val dense2 = DenseVector[Float](densefloatValues)
    blist.add(dense2)
    ilist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    val hash2 = new HashVector[Float](new OpenAddressHashArray[Float](dim))
    intrandIndices.zip(floatValues).foreach { case (i, v) => hash2(i) = v }
    blist.add(hash2)
    ilist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))
    val sorted2 = new SparseVector[Float](new SparseArray(intsortedIndices, floatValues, capacity, dim, 0.0f))
    blist.add(sorted2)

    ilist.add(VFactory.denseLongVector(denselongValues))
    val dense3 = DenseVector[Long](denselongValues)
    blist.add(dense3)
    ilist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    val hash3 = new HashVector[Long](new OpenAddressHashArray[Long](dim))
    intrandIndices.zip(longValues).foreach { case (i, v) => hash3(i) = v }
    blist.add(hash3)
    ilist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))
    val sorted3 = new SparseVector[Long](new SparseArray(intsortedIndices, longValues, capacity, dim, 0l))
    blist.add(sorted3)

    ilist.add(VFactory.denseIntVector(denseintValues))
    val dense4 = DenseVector[Int](denseintValues)
    blist.add()
    ilist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    val hash4 = new HashVector[Int](new OpenAddressHashArray[Int](dim))
    intrandIndices.zip(intValues).foreach { case (i, v) => hash4(i) = v }
    blist.add(hash4)
    ilist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))
    val sorted4 = new SparseVector[Int](new SparseArray(intsortedIndices, intValues, capacity, dim, 0))
    blist.add(sorted4)


    //ilist.add(VFactory.intDummyVector(dim, intsortedIndices))

    println(s"${ilist.get(0).getClass.getSimpleName}: ${getFlag(ilist.get(0))} dot ${ilist.get(0).getClass.getSimpleName}: ${ilist.get(0).getClass.getSimpleName} is ${ilist.get(0).dot(ilist.get(0))}, and breeze is ${dense1.dot(dense1)}")
    println(s"${ilist.get(1).getClass.getSimpleName}: ${getFlag(ilist.get(1))} dot ${ilist.get(1).getClass.getSimpleName}: ${ilist.get(1).getClass.getSimpleName} is ${ilist.get(1).dot(ilist.get(1))}, and breeze is ${hash1.dot(hash1)}")
    println(s"${ilist.get(2).getClass.getSimpleName}: ${getFlag(ilist.get(2))} dot ${ilist.get(2).getClass.getSimpleName}: ${ilist.get(2).getClass.getSimpleName} is ${ilist.get(2).dot(ilist.get(2))}, and breeze is ${sorted1.dot(sorted1)}")
    println(s"${ilist.get(3).getClass.getSimpleName}: ${getFlag(ilist.get(3))} dot ${ilist.get(3).getClass.getSimpleName}: ${ilist.get(3).getClass.getSimpleName} is ${ilist.get(3).dot(ilist.get(3))}, and breeze is ${dense2.dot(dense2)}")
    println(s"${ilist.get(4).getClass.getSimpleName}: ${getFlag(ilist.get(4))} dot ${ilist.get(4).getClass.getSimpleName}: ${ilist.get(4).getClass.getSimpleName} is ${ilist.get(4).dot(ilist.get(4))}, and breeze is ${hash2.dot(hash2)}")
    println(s"${ilist.get(5).getClass.getSimpleName}: ${getFlag(ilist.get(5))} dot ${ilist.get(5).getClass.getSimpleName}: ${ilist.get(5).getClass.getSimpleName} is ${ilist.get(5).dot(ilist.get(5))}, and breeze is ${sorted2.dot(sorted2)}")
    println(s"${ilist.get(6).getClass.getSimpleName}: ${getFlag(ilist.get(6))} dot ${ilist.get(6).getClass.getSimpleName}: ${ilist.get(6).getClass.getSimpleName} is ${ilist.get(6).dot(ilist.get(6))}, and breeze is ${dense3.dot(dense3)}")
    println(s"${ilist.get(7).getClass.getSimpleName}: ${getFlag(ilist.get(7))} dot ${ilist.get(7).getClass.getSimpleName}: ${ilist.get(7).getClass.getSimpleName} is ${ilist.get(7).dot(ilist.get(7))}, and breeze is ${hash3.dot(hash3)}")
    println(s"${ilist.get(8).getClass.getSimpleName}: ${getFlag(ilist.get(8))} dot ${ilist.get(8).getClass.getSimpleName}: ${ilist.get(8).getClass.getSimpleName} is ${ilist.get(8).dot(ilist.get(8))}, and breeze is ${sorted3.dot(sorted3)}")
    println(s"${ilist.get(9).getClass.getSimpleName}: ${getFlag(ilist.get(9))} dot ${ilist.get(9).getClass.getSimpleName}: ${ilist.get(9).getClass.getSimpleName} is ${ilist.get(9).dot(ilist.get(9))}, and breeze is ${dense4.dot(dense4)}")
    println(s"${ilist.get(10).getClass.getSimpleName}: ${getFlag(ilist.get(10))} dot ${ilist.get(10).getClass.getSimpleName}: ${ilist.get(10).getClass.getSimpleName} is ${ilist.get(10).dot(ilist.get(10))}, and breeze is ${hash4.dot(hash4)}")
    println(s"${ilist.get(11).getClass.getSimpleName}: ${getFlag(ilist.get(11))} dot ${ilist.get(11).getClass.getSimpleName}: ${ilist.get(11).getClass.getSimpleName} is ${ilist.get(11).dot(ilist.get(11))}, and breeze is ${sorted4.dot(sorted4)}")

    val times = 50000
    val start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      (0 until ilist.size()).foreach { i =>
        ilist.get(i).dot(ilist.get(i))
      }
    }
    val stop1 = System.currentTimeMillis()
    val cost1 = stop1 - start1

    println()

    val start2 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      dense1.dot(dense1)
      hash1.dot(hash1)
      sorted1.dot(sorted1)
      dense2.dot(dense2)
      hash2.dot(hash2)
      sorted2.dot(sorted2)
      dense3.dot(dense3)
      hash3.dot(hash3)
      sorted3.dot(sorted3)
      dense4.dot(dense4)
      hash4.dot(hash4)
      sorted4.dot(sorted4)
    }
    val stop2 = System.currentTimeMillis()
    val cost2 = stop2 - start2
    println()

    println(s"angel:$cost1, breeze:$cost2, ratio:${1.0 * cost2 / cost1}")


  }

  def dotLongKeyVector(): Unit = {
    val llist = new util.ArrayList[Vector]()

    llist.add(VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues))
    llist.add(VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues))

    llist.add(VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues))
    llist.add(VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues))

    llist.add(VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues))
    llist.add(VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues))

    llist.add(VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues))
    llist.add(VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues))

    llist.add(VFactory.longDummyVector(dim, longsortedIndices))

    (0 until llist.size()).foreach { i =>
      val v1 = llist.get(i)
      (0 until llist.size()).foreach { j =>
        val v2 = llist.get(j)

        println(
          s"${v1.getClass.getSimpleName}:${getFlag(v1)} dot ${v2.getClass.getSimpleName}:${v2.getClass.getSimpleName} is ${v1.dot(v2)}"
        )
      }
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

  def dot(v1: Any, v2: Any): Double = {
    (v1, v2) match {
      case (x1: DenseVector[Double], x2: DenseVector[Double]) => x1.dot(x2)
      case (x1: DenseVector[Float], x2: DenseVector[Float]) => x1.dot(x2)
      case (x1: DenseVector[Long], x2: DenseVector[Long]) => x1.dot(x2)
      case (x1: DenseVector[Int], x2: DenseVector[Int]) => x1.dot(x2)
      case (x1: DenseVector[Double], x2: HashVector[Double]) => x1.dot(x2)
      case (x1: DenseVector[Float], x2: HashVector[Float]) => x1.dot(x2)
      case (x1: DenseVector[Long], x2: HashVector[Long]) => x1.dot(x2)
      case (x1: DenseVector[Int], x2: HashVector[Int]) => x1.dot(x2)
      case (x1: DenseVector[Double], x2: SparseVector[Double]) => x1.dot(x2)
      case (x1: DenseVector[Float], x2: SparseVector[Float]) => x1.dot(x2)
      case (x1: DenseVector[Long], x2: SparseVector[Long]) => x1.dot(x2)
      case (x1: DenseVector[Int], x2: SparseVector[Int]) => x1.dot(x2)
      case (x1: HashVector[Double], x2: DenseVector[Double]) => x1.dot(x2)
      case (x1: HashVector[Float], x2: DenseVector[Float]) => x1.dot(x2)
      case (x1: HashVector[Long], x2: DenseVector[Long]) => x1.dot(x2)
      case (x1: HashVector[Int], x2: DenseVector[Int]) => x1.dot(x2)
      case (x1: HashVector[Double], x2: HashVector[Double]) => x1.dot(x2)
      case (x1: HashVector[Float], x2: HashVector[Float]) => x1.dot(x2)
      case (x1: HashVector[Long], x2: HashVector[Long]) => x1.dot(x2)
      case (x1: HashVector[Int], x2: HashVector[Int]) => x1.dot(x2)
      case (x1: HashVector[Double], x2: SparseVector[Double]) => x1.dot(x2)
      case (x1: HashVector[Float], x2: SparseVector[Float]) => x1.dot(x2)
      case (x1: HashVector[Long], x2: SparseVector[Long]) => x1.dot(x2)
      case (x1: HashVector[Int], x2: SparseVector[Int]) => x1.dot(x2)
      case (x1: SparseVector[Double], x2: DenseVector[Double]) => x1.dot(x2)
      case (x1: SparseVector[Float], x2: DenseVector[Float]) => x1.dot(x2)
      case (x1: SparseVector[Long], x2: DenseVector[Long]) => x1.dot(x2)
      case (x1: SparseVector[Int], x2: DenseVector[Int]) => x1.dot(x2)
      case (x1: SparseVector[Double], x2: HashVector[Double]) => x1.dot(x2)
      case (x1: SparseVector[Float], x2: HashVector[Float]) => x1.dot(x2)
      case (x1: SparseVector[Long], x2: HashVector[Long]) => x1.dot(x2)
      case (x1: SparseVector[Int], x2: HashVector[Int]) => x1.dot(x2)
      case (x1: SparseVector[Double], x2: SparseVector[Double]) => x1.dot(x2)
      case (x1: SparseVector[Float], x2: SparseVector[Float]) => x1.dot(x2)
      case (x1: SparseVector[Long], x2: SparseVector[Long]) => x1.dot(x2)
      case (x1: SparseVector[Int], x2: SparseVector[Int]) => x1.dot(x2)
    }
  }
}
