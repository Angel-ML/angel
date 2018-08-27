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


package com.tencent.angel.ml.math2.matrix.BlasMatrixTest

import java.util

import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import org.junit.{BeforeClass, Test}
import org.scalatest.FunSuite

object BlasMatrixBaseFunTest {
  val matrixId = 0
  val rowId = 0
  val clock = 0
  val capacity: Int = 100
  val dim: Int = capacity * 10

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

  val densedoubleMatrixValues: Array[Double] = new Array[Double](1000 * 1000)
  val densefloatMatrixValues: Array[Float] = new Array[Float](1000 * 1000)

  val matrixlist = new util.ArrayList[Matrix]()
  val vlist = new util.ArrayList[Vector]()
  var densematrix1 = MFactory.denseDoubleMatrix(1000, 1000, densedoubleValues)
  var densematrix2 = MFactory.denseFloatMatrix(1000, 1000, densefloatValues)

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

    densedoubleValues.indices.foreach { j =>
      densedoubleValues(j) = rand.nextDouble() + 0.01
    }

    densefloatValues.indices.foreach { i =>
      densefloatValues(i) = rand.nextFloat() + 0.01f
    }

    denselongValues.indices.foreach { i =>
      denselongValues(i) = rand.nextInt(100) + 1L
    }

    denseintValues.indices.foreach { i =>
      denseintValues(i) = rand.nextInt(100) + 1
    }

    densedoubleMatrixValues.indices.foreach { j =>
      densedoubleMatrixValues(j) = rand.nextDouble() + 0.01
    }

    densefloatMatrixValues.indices.foreach { i =>
      densefloatMatrixValues(i) = rand.nextFloat() + 0.01f
    }

    densematrix1 = MFactory.denseDoubleMatrix(1000, 1000, densedoubleMatrixValues)
    densematrix2 = MFactory.denseFloatMatrix(1000, 1000, densefloatMatrixValues)


    vlist.add(VFactory.denseDoubleVector(densedoubleValues))
    vlist.add(VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues))
    vlist.add(VFactory.sortedDoubleVector(dim, intsortedIndices, doubleValues))
    vlist.add(VFactory.denseFloatVector(densefloatValues))
    vlist.add(VFactory.sparseFloatVector(dim, intrandIndices, floatValues))
    vlist.add(VFactory.sortedFloatVector(dim, intsortedIndices, floatValues))
    vlist.add(VFactory.denseLongVector(denselongValues))
    vlist.add(VFactory.sparseLongVector(dim, intrandIndices, longValues))
    vlist.add(VFactory.sortedLongVector(dim, intsortedIndices, longValues))
    vlist.add(VFactory.denseIntVector(denseintValues))
    vlist.add(VFactory.sparseIntVector(dim, intrandIndices, intValues))
    vlist.add(VFactory.sortedIntVector(dim, intsortedIndices, intValues))
    vlist.add(VFactory.intDummyVector(dim, intsortedIndices))

  }
}


class BlasMatrixBaseFunTest {
  val matrixlist = BlasMatrixBaseFunTest.matrixlist
  val vlist = BlasMatrixBaseFunTest.vlist
  var densematrix1 = BlasMatrixBaseFunTest.densematrix1
  var densematrix2 = BlasMatrixBaseFunTest.densematrix2

  @Test
  def BlasDoubleMatrixTest() {
    //diag
    println(s"diag ${densematrix1.diag().sum()}")

    //clone
    val densenatrix_clone = densematrix1.copy()
    assert(densenatrix_clone != densematrix1)

    //get,set
    val x = densematrix1.get(0, 0)
    densematrix1.set(0, 0, 10)
    println(s"get(0,0): ${x}, set(0,0): ${densematrix1.get(0, 0)}")

    //getRow,setRow
    (0 until vlist.size()).foreach { i =>
      val vr = densematrix1.getRow(i)
      densematrix1.setRow(i, vlist.get(i))
      println(s"getRow($i): ${vr.sum()}, setRow($i): ${densematrix1.getRow(i).sum()}, ${vlist.get(i).sum()}")
    }

    //getCol, setCol
    (0 until vlist.size()).foreach { i =>
      val vc = densematrix1.getCol(i)
      densematrix1.setCol(i, vlist.get(i))
      println(s"getCol($i): ${vc.sum()}, setCol($i): ${densematrix1.getCol(i).sum()}, ${vlist.get(i).sum()}")
    }

    densematrix1.reshape(100, 10000)
    println(densematrix1.getNumRows, densematrix1.getNumCols)

    densematrix1.clear()
    println(s"clear: ${densematrix1.sum()}")
  }


  @Test
  def BlasFloatMatrixTest() {
    //diag
    println(s"diag ${densematrix2.diag().sum()}")

    //clone
    val densenatrix_clone = densematrix2.copy()
    assert(densenatrix_clone != densematrix2)

    //get, set
    val x = densematrix2.get(0, 0)
    densematrix2.set(0, 0, 10)
    println(s"get(0,0): ${x}, set(0,0): ${densematrix2.get(0, 0)}")

    //getRow, setRow
    (3 until vlist.size()).foreach { i =>
      val vr = densematrix2.getRow(i)
      densematrix2.setRow(i, vlist.get(i))
      println(s"getRow($i): ${vr.sum()}, setRow($i): ${densematrix2.getRow(i).sum()}, ${vlist.get(i).sum()}")
    }

    //getCol, setCol
    (3 until vlist.size()).foreach { i =>
      val vc = densematrix2.getCol(i)
      densematrix2.setCol(i, vlist.get(i))
      println(s"getCol($i): ${vc.sum()}, setCol($i): ${densematrix2.getCol(i).sum()}, ${vlist.get(i).sum()}")
    }

    densematrix2.reshape(100, 10000)
    println(densematrix2.getNumRows, densematrix2.getNumCols)

    densematrix2.clear()
    println(s"clear: ${densematrix2.sum()}")
  }
}
