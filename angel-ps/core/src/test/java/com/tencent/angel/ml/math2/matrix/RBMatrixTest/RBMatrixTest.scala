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


package com.tencent.angel.ml.math2.matrix.RBMatrixTest

import java.util

import breeze.numerics.abs
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{IntDummyVector, LongDummyVector, Vector}
import org.junit.{BeforeClass, Test}

object RBMatrixTest {
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

  var intdummy = VFactory.intDummyVector(dim, intsortedIndices)
  var longdummy = VFactory.longDummyVector(dim, longsortedIndices)

  val matrixlist = new util.ArrayList[Matrix]()
  val vectorlist = new util.ArrayList[Vector]()

  val lmatrixlist = new util.ArrayList[Matrix]()
  val lvectorlist = new util.ArrayList[Vector]()

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

    val doubledense = VFactory.denseDoubleVector(densedoubleValues)
    val doublesparse = VFactory.sparseDoubleVector(dim, intrandIndices, doubleValues)
    val doublesorted = VFactory.sortedDoubleVector(dim, capacity, intsortedIndices, doubleValues)

    val floatdense = VFactory.denseFloatVector(densefloatValues)
    val floatsparse = VFactory.sparseFloatVector(dim, intrandIndices, floatValues)
    val floatsorted = VFactory.sortedFloatVector(dim, capacity, intsortedIndices, floatValues)

    val longdense = VFactory.denseLongVector(denselongValues)
    val longsparse = VFactory.sparseLongVector(dim, intrandIndices, longValues)
    val longsorted = VFactory.sortedLongVector(dim, capacity, intsortedIndices, longValues)

    val intdense = VFactory.denseIntVector(denseintValues)
    val intsparse = VFactory.sparseIntVector(dim, intrandIndices, intValues)
    val intsorted = VFactory.sortedIntVector(dim, capacity, intsortedIndices, intValues)

    intdummy = VFactory.intDummyVector(dim, intsortedIndices)
    longdummy = VFactory.longDummyVector(dim, longsortedIndices)


    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doubledense, doublesparse, doublesorted)))
    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doubledense, doublesorted, doublesparse)))
    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doublesparse, doubledense, doublesorted)))
    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doublesparse, doublesorted, doubledense)))
    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doublesorted, doublesparse, doubledense)))
    matrixlist.add(MFactory.rbIntDoubleMatrix(Array(doublesorted, doubledense, doublesparse)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatdense, floatsparse, floatsorted)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatdense, floatsorted, floatsparse)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatsparse, floatdense, floatsorted)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatsparse, floatsorted, floatdense)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatsorted, floatdense, floatsparse)))
    matrixlist.add(MFactory.rbIntFloatMatrix(Array(floatsorted, floatsparse, floatdense)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longdense, longsparse, longsorted)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longdense, longsorted, longsparse)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longsparse, longdense, longsorted)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longsparse, longsorted, longdense)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longsorted, longdense, longsparse)))
    matrixlist.add(MFactory.rbIntLongMatrix(Array(longsorted, longsparse, longdense)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intdense, intsparse, intsorted)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intdense, intsorted, intsparse)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intsparse, intdense, intsorted)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intsparse, intsorted, intdense)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intsorted, intdense, intsparse)))
    matrixlist.add(MFactory.rbIntIntMatrix(Array(intsorted, intsparse, intdense)))

    vectorlist.add(doubledense)
    vectorlist.add(doublesparse)
    vectorlist.add(doublesorted)
    vectorlist.add(floatdense)
    vectorlist.add(floatsparse)
    vectorlist.add(floatsorted)
    vectorlist.add(longdense)
    vectorlist.add(longsparse)
    vectorlist.add(longsorted)
    vectorlist.add(intdense)
    vectorlist.add(intsparse)
    vectorlist.add(intsorted)
    vectorlist.add(intdummy)


    val ldoublesparse = VFactory.sparseLongKeyDoubleVector(dim, longrandIndices, doubleValues)
    val ldoublesorted = VFactory.sortedLongKeyDoubleVector(dim, capacity, longsortedIndices, doubleValues)

    val lfloatsparse = VFactory.sparseLongKeyFloatVector(dim, longrandIndices, floatValues)
    val lfloatsorted = VFactory.sortedLongKeyFloatVector(dim, capacity, longsortedIndices, floatValues)

    val llongsparse = VFactory.sparseLongKeyLongVector(dim, longrandIndices, longValues)
    val llongsorted = VFactory.sortedLongKeyLongVector(dim, capacity, longsortedIndices, longValues)

    val lintsparse = VFactory.sparseLongKeyIntVector(dim, longrandIndices, intValues)
    val lintsorted = VFactory.sortedLongKeyIntVector(dim, capacity, longsortedIndices, intValues)

    lmatrixlist.add(MFactory.rbLongDoubleMatrix(Array(ldoublesparse, ldoublesorted)))
    lmatrixlist.add(MFactory.rbLongDoubleMatrix(Array(ldoublesorted, ldoublesparse)))
    lmatrixlist.add(MFactory.rbLongFloatMatrix(Array(lfloatsparse, lfloatsorted)))
    lmatrixlist.add(MFactory.rbLongFloatMatrix(Array(lfloatsorted, lfloatsparse)))
    lmatrixlist.add(MFactory.rbLongLongMatrix(Array(llongsparse, llongsorted)))
    lmatrixlist.add(MFactory.rbLongLongMatrix(Array(llongsorted, llongsparse)))
    lmatrixlist.add(MFactory.rbLongIntMatrix(Array(lintsparse, lintsorted)))
    lmatrixlist.add(MFactory.rbLongIntMatrix(Array(lintsorted, lintsparse)))

    lvectorlist.add(ldoublesparse)
    lvectorlist.add(ldoublesorted)
    lvectorlist.add(lfloatsparse)
    lvectorlist.add(lfloatsorted)
    lvectorlist.add(llongsparse)
    lvectorlist.add(llongsorted)
    lvectorlist.add(lintsparse)
    lvectorlist.add(lintsorted)
    lvectorlist.add(longdummy)

  }
}

class RBMatrixTest {
  val matrixlist = RBMatrixTest.matrixlist
  val vectorlist = RBMatrixTest.vectorlist

  val lmatrixlist = RBMatrixTest.lmatrixlist
  val lvectorlist = RBMatrixTest.lvectorlist
  val intdummy = RBMatrixTest.intdummy
  val longdummy = RBMatrixTest.longdummy

  @Test
  def addTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).add(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).add(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      matrixlist.get(i).add(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).add(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).add(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
  }

  @Test
  def subTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
         matrixlist.get(i).sub(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).sub(vectorlist.get(j)).sum() - (matrixlist.get(i).sum() - vectorlist.get(j).sum())
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
     matrixlist.get(i).sub(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).sub(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).sub(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
    }
  }

  @Test
  def mulTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).mul(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).mul(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      matrixlist.get(i).mul(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).mul(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).mul(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
        }
      }
      lmatrixlist.get(i).mul(2).sum()
    }
  }

  @Test
  def divTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).div(matrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).div(vectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
     matrixlist.get(i).div(2).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).div(lmatrixlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).div(lvectorlist.get(j)).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
      lmatrixlist.get(i).div(2).sum()
    }
  }

  @Test
  def axpyTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          matrixlist.get(i).axpy(matrixlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
           e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).axpy(vectorlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).axpy(lmatrixlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
         lmatrixlist.get(i).axpy(lvectorlist.get(j), 2.0).sum()
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
    }
  }

  @Test
  def dotTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until vectorlist.size).foreach { j =>
        try {
          matrixlist.get(i).dot(vectorlist.get(j))
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lvectorlist.size).foreach { j =>
        try {
          lmatrixlist.get(i).dot(lvectorlist.get(j))
        } catch {
          case e: AngelException => {
            e
          }
          case e: ArithmeticException => {
            e
          }
        }
      }
    }
  }

  @Test
  def reduceTest() {
    (0 until matrixlist.size).foreach { i =>
      matrixlist.get(i).norm()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      lmatrixlist.get(i).norm()
    }
  }

  @Test
  def diagTest() {
    (0 until matrixlist.size).foreach { i =>
      matrixlist.get(i).diag().sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      lmatrixlist.get(i).diag().sum()
    }
  }

  @Test
  def expTest() {
    (0 until matrixlist.size).foreach { i =>
      Ufuncs.exp(matrixlist.get(i)).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        Ufuncs.exp(lmatrixlist.get(i)).sum()
      } catch {
        case e: AngelException => {
          e
        }
      }
    }
  }

  @Test
  def logTest() {
    (0 until matrixlist.size).foreach { i =>
     Ufuncs.log(matrixlist.get(i)).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        Ufuncs.log(lmatrixlist.get(i)).sum()
      } catch {
        case e: AngelException => {
         e
        }
      }
    }
  }

  @Test
  def log1pTest() {
    (0 until matrixlist.size).foreach { i =>
      Ufuncs.log1p(matrixlist.get(i)).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        Ufuncs.log1p(lmatrixlist.get(i)).sum()
      } catch {
        case e: AngelException => {
         e
        }
      }
    }
  }

  @Test
  def powTest() {
    (0 until matrixlist.size).foreach { i =>
     Ufuncs.pow(matrixlist.get(i), 2.0).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
     Ufuncs.pow(lmatrixlist.get(i), 2.0).sum()
    }
  }

  @Test
  def sigmoidTest() {
    (0 until matrixlist.size).foreach { i =>
     TransFuncs.sigmoid(matrixlist.get(i)).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        TransFuncs.sigmoid(lmatrixlist.get(i)).sum()
      } catch {
        case e: AngelException => {
          e
        }
      }
    }
  }

  @Test
  def sqrtTest() {
    (0 until matrixlist.size).foreach { i =>
      Ufuncs.sqrt(matrixlist.get(i)).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      Ufuncs.sqrt(lmatrixlist.get(i)).sum()
    }
  }

  @Test
  def softthresholdTest() {
    (0 until matrixlist.size).foreach { i =>
      Ufuncs.softthreshold(matrixlist.get(i), 2.0).sum()
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      Ufuncs.softthreshold(lmatrixlist.get(i), 2.0).sum()
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
