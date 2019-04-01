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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{CompIntDoubleVector, CompIntFloatVector, CompIntIntVector, CompIntLongVector, CompLongDoubleVector, CompLongFloatVector, CompLongIntVector, CompLongLongVector, Vector}
import org.junit.{BeforeClass, Test}


object CompUnaryOPTest {
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
  val slist = new util.ArrayList[Vector]()
  val llist = new util.ArrayList[Vector]()
  val sllist = new util.ArrayList[Vector]()

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

    val dense1 = VFactory.denseDoubleVector(densedoubleValues)
    val sparse1 = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val sorted1 = VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues)
    val list1 = Array(dense1, sparse1, sorted1)
    val comp1 = new CompIntDoubleVector(dim * list1.length, list1)

    slist.add(dense1)
    slist.add(sparse1)
    slist.add(sorted1)
    list.add(comp1)

    val dense2 = VFactory.denseFloatVector(densefloatValues)
    val sparse2 = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val sorted2 = VFactory.sortedFloatVector(dim, intsortedIndices, floatValues)
    val list2 = Array(dense2, sparse2, sorted2)
    val comp2 = new CompIntFloatVector(dim * list2.length, list2)

    slist.add(dense2)
    slist.add(sparse2)
    slist.add(sorted2)
    list.add(comp2)

    val dense3 = VFactory.denseLongVector(denselongValues)
    val sparse3 = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val sorted3 = VFactory.sortedLongVector(dim, intsortedIndices, longValues)
    val list3 = Array(dense3, sparse3, sorted3)
    val comp3 = new CompIntLongVector(dim * list3.length, list3)

    slist.add(dense3)
    slist.add(sparse3)
    slist.add(sorted3)
    list.add(comp3)

    val dense4 = VFactory.denseIntVector(denseintValues)
    val sparse4 = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val sorted4 = VFactory.sortedIntVector(dim, intsortedIndices, intValues)
    val list4 = Array(dense4, sparse4, sorted4)
    val comp4 = new CompIntIntVector(dim * list4.length, list4)

    slist.add(dense4)
    slist.add(sparse4)
    slist.add(sorted4)
    list.add(comp4)

    val lsparse1 = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val lsorted1 = VFactory.sortedLongKeyDoubleVector(dim, longsortedIndices, doubleValues)
    val llist1 = Array(lsparse1, lsorted1)
    val lcomp1 = new CompLongDoubleVector(dim * llist1.length, llist1)

    sllist.add(lsparse1)
    sllist.add(lsorted1)
    llist.add(lcomp1)

    val lsparse2 = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lsorted2 = VFactory.sortedLongKeyFloatVector(dim, longsortedIndices, floatValues)
    val llist2 = Array(lsparse2, lsorted2)
    val lcomp2 = new CompLongFloatVector(dim * llist2.length, llist2)

    sllist.add(lsparse2)
    sllist.add(lsorted2)
    llist.add(lcomp2)

    val lsparse3 = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val lsorted3 = VFactory.sortedLongKeyLongVector(dim, longsortedIndices, longValues)
    val llist3 = Array(lsparse3, lsorted3)
    val lcomp3 = new CompLongLongVector(dim * llist3.length, llist3)

    sllist.add(lsparse3)
    sllist.add(lsorted3)
    llist.add(lcomp3)

    val lsparse4 = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lsorted4 = VFactory.sortedLongKeyIntVector(dim, longsortedIndices, intValues)
    val llist4 = Array(lsparse4, lsorted4)
    val lcomp4 = new CompLongIntVector(dim * llist4.length, llist4)

    sllist.add(lsparse4)
    sllist.add(lsorted4)
    llist.add(lcomp4)
  }
}

class CompUnaryOPTest {
  val list = CompUnaryOPTest.list
  val slist = CompUnaryOPTest.slist
  val llist = CompUnaryOPTest.llist
  val sllist = CompUnaryOPTest.sllist

  val times = 50
  var start1, stop1, cost1, start2, stop2, cost2 = 0L

  @Test
  def expTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.exp(list.get(0))
      Ufuncs.exp(list.get(1))
      Ufuncs.exp(list.get(2))
      Ufuncs.exp(list.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp exp:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.exp(list.get(i))
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(abs(Ufuncs.exp(list.get(i)).sum() - (Ufuncs.exp(slist.get(j)).sum() + Ufuncs.exp(slist.get(j+1)).sum() + Ufuncs.exp(slist.get(j+2)).sum()))<1.0)
      //      }
    }
  }

  @Test
  def logTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log(list.get(0))
      Ufuncs.log(list.get(1))
      Ufuncs.log(list.get(2))
      Ufuncs.log(list.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp log:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.log(list.get(i))
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.log(list.get(i)).sum() == Ufuncs.log(slist.get(j)).sum() + Ufuncs.log(slist.get(j+1)).sum() + Ufuncs.log(slist.get(j+2)).sum())
      //      }
    }
  }

  @Test
  def log1pTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.log1p(list.get(0))
      Ufuncs.log1p(list.get(1))
      Ufuncs.log1p(list.get(2))
      Ufuncs.log1p(list.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp log1p:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.log1p(list.get(i))
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.log1p(list.get(i)).sum() == Ufuncs.log1p(slist.get(j)).sum() + Ufuncs.log1p(slist.get(j+1)).sum() + Ufuncs.log1p(slist.get(j+2)).sum())
      //      }
    }
  }

  @Test
  def sigmoidTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      TransFuncs.sigmoid(list.get(0))
      TransFuncs.sigmoid(list.get(1))
      TransFuncs.sigmoid(list.get(2))
      TransFuncs.sigmoid(list.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp sigmoid:$cost1")

    (0 until list.size()).foreach { i =>
      TransFuncs.sigmoid(list.get(i))
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.sigmoid(list.get(i)).sum() == Ufuncs.sigmoid(slist.get(j)).sum() + Ufuncs.sigmoid(slist.get(j+1)).sum() + Ufuncs.sigmoid(slist.get(j+2)).sum())
      //      }
    }
  }

  @Test
  def softthresholdTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.softthreshold(list.get(0), 2.0)
      Ufuncs.softthreshold(list.get(1), 2.0)
      Ufuncs.softthreshold(list.get(2), 2.0)
      Ufuncs.softthreshold(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp softthreshold:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.softthreshold(list.get(i), 2.0)
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.softthreshold(list.get(i), 2.0).sum() == Ufuncs.softthreshold(slist.get(j), 2.0).sum() + Ufuncs.softthreshold(slist.get(j+1), 2.0).sum() + Ufuncs.softthreshold(slist.get(j+2), 2.0).sum())
      //      }
    }
  }

  @Test
  def saddTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sadd(list.get(0), 2.0)
      Ufuncs.sadd(list.get(1), 2.0)
      Ufuncs.sadd(list.get(2), 2.0)
      Ufuncs.sadd(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp sadd:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.sadd(list.get(i), 2.0)
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.sadd(list.get(i), 2.0).sum() == Ufuncs.sadd(slist.get(j), 2.0).sum() + Ufuncs.sadd(slist.get(j+1), 2.0).sum() + Ufuncs.sadd(slist.get(j+2), 2.0).sum())
      //      }
    }
  }

  @Test
  def ssubTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.ssub(list.get(0), 2.0)
      Ufuncs.ssub(list.get(1), 2.0)
      Ufuncs.ssub(list.get(2), 2.0)
      Ufuncs.ssub(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp ssub:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.ssub(list.get(i), 2.0)
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.ssub(list.get(i), 2.0).sum() == Ufuncs.ssub(slist.get(j), 2.0).sum() + Ufuncs.ssub(slist.get(j+1), 2.0).sum() + Ufuncs.ssub(slist.get(j+2), 2.0).sum())
      //      }
    }
  }

  @Test
  def powTest() {
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.pow(list.get(0), 2.0)
      Ufuncs.pow(list.get(1), 2.0)
      Ufuncs.pow(list.get(2), 2.0)
      Ufuncs.pow(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp pow:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.pow(list.get(i), 2.0)
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.pow(list.get(i), 2.0).sum() == Ufuncs.pow(slist.get(j), 2.0).sum() + Ufuncs.pow(slist.get(j+1), 2.0).sum() + Ufuncs.pow(slist.get(j+2), 2.0).sum())
      //      }
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
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sqrt(list.get(0))
      Ufuncs.sqrt(list.get(1))
      Ufuncs.sqrt(list.get(2))
      Ufuncs.sqrt(list.get(3))
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp sqrt:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.sqrt(list.get(i))
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.sqrt(list.get(i)).sum() == Ufuncs.sqrt(slist.get(j)).sum() + Ufuncs.sqrt(slist.get(j+1)).sum() + Ufuncs.sqrt(slist.get(j+2)).sum())
      //      }
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
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.smul(list.get(0), 2.0)
      Ufuncs.smul(list.get(1), 2.0)
      Ufuncs.smul(list.get(2), 2.0)
      Ufuncs.smul(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp smul:$cost1")

    val tmp = (0 until list.size()).map { i =>
      Ufuncs.smul(list.get(i), 2.0)

      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.smul(list.get(i), 2.0).sum() == Ufuncs.smul(slist.get(j), 2.0).sum() + Ufuncs.smul(slist.get(j+1), 2.0).sum() + Ufuncs.smul(slist.get(j+2), 2.0).sum())
      //      }
    }

    tmp.foreach(t => t.sum())

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
    start1 = System.currentTimeMillis()
    (0 to times).foreach { _ =>
      Ufuncs.sdiv(list.get(0), 2.0)
      Ufuncs.sdiv(list.get(1), 2.0)
      Ufuncs.sdiv(list.get(2), 2.0)
      Ufuncs.sdiv(list.get(3), 2.0)
    }
    stop1 = System.currentTimeMillis()
    cost1 = stop1 - start1
    println(s"angel comp sdiv:$cost1")

    (0 until list.size()).foreach { i =>
      Ufuncs.sdiv(list.get(i), 2.0)
      //      (i*3 until slist.size()-2).foreach{ j =>
      //        assert(Ufuncs.sdiv(list.get(i), 2.0).sum() == Ufuncs.sdiv(slist.get(j), 2.0).sum() + Ufuncs.sdiv(slist.get(j+1), 2.0).sum() + Ufuncs.sdiv(slist.get(j+2), 2.0).sum())
      //      }
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
}
