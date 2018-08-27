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
          println(s"matrix add matrix: ${matrixlist.get(i).add(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix add vector: ${matrixlist.get(i).add(vectorlist.get(j)).sum()}")
          if (getFlag(vectorlist.get(j)) != "dummy") {
            assert(abs(matrixlist.get(i).add(vectorlist.get(j)).sum() - (matrixlist.get(i).sum() + vectorlist.get(j).sum())) < 1.0E-2)
          } else {
            assert(abs(matrixlist.get(i).add(vectorlist.get(j)).sum() - (matrixlist.get(i).sum() + intdummy.sum())) < 1.0E-4)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix sadd: ${matrixlist.get(i).add(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix add matrix: ${lmatrixlist.get(i).add(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix add vector: ${lmatrixlist.get(i).add(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
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
          println(s"matrix sub matrix: ${matrixlist.get(i).sub(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix sub vector: ${matrixlist.get(i).sub(vectorlist.get(j)).sum()}")
          if (getFlag(vectorlist.get(j)) != "dummy") {
            assert(abs(matrixlist.get(i).sub(vectorlist.get(j)).sum() - (matrixlist.get(i).sum() - vectorlist.get(j).sum())) < 1.0E-2)
          } else {
            assert(abs(matrixlist.get(i).sub(vectorlist.get(j)).sum() - (matrixlist.get(i).sum() - intdummy.sum())) < 1.0E-4)
          }
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix ssub: ${matrixlist.get(i).sub(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix sub matrix: ${lmatrixlist.get(i).sub(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix sub vector: ${lmatrixlist.get(i).sub(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
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
          println(s"matrix mul matrix: ${matrixlist.get(i).mul(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix mul vector: ${matrixlist.get(i).mul(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix smul: ${matrixlist.get(i).mul(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix mul matrix: ${lmatrixlist.get(i).mul(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix mul vector: ${lmatrixlist.get(i).mul(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix smul: ${lmatrixlist.get(i).mul(2).sum()}")
    }
  }

  @Test
  def divTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix div matrix: ${matrixlist.get(i).div(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix div vector: ${matrixlist.get(i).div(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      println(s"matrix sdiv: ${matrixlist.get(i).div(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix div matrix: ${lmatrixlist.get(i).div(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix div vector: ${lmatrixlist.get(i).div(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      println(s"matrix sdiv: ${lmatrixlist.get(i).div(2).sum()}")
    }
  }

  @Test
  def axpyTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix axpy matrix: ${matrixlist.get(i).axpy(matrixlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix axpy vector: ${matrixlist.get(i).axpy(vectorlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix axpy matrix: ${lmatrixlist.get(i).axpy(lmatrixlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix axpy vector: ${lmatrixlist.get(i).axpy(lvectorlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
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
          println(s"matrix dot vector: ${matrixlist.get(i).dot(vectorlist.get(j))}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix dot vector: ${lmatrixlist.get(i).dot(lvectorlist.get(j))}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def reduceTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix sum: ${matrixlist.get(i).sum()},matrix average: ${matrixlist.get(i).average()}, matrix std: ${matrixlist.get(i).std()},matrix norm: ${matrixlist.get(i).norm()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix sum: ${lmatrixlist.get(i).sum()},matrix average: ${lmatrixlist.get(i).average()}, matrix std: ${lmatrixlist.get(i).std()},matrix norm: ${lmatrixlist.get(i).norm()}")
    }
  }

  @Test
  def diagTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix diag sum: ${matrixlist.get(i).diag().sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix diag sum: ${lmatrixlist.get(i).diag().sum()}")
    }
  }

  @Test
  def expTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix exp sum: ${Ufuncs.exp(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix exp sum: ${Ufuncs.exp(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def logTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix log sum: ${Ufuncs.log(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix log sum: ${Ufuncs.log(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def log1pTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix log1p sum: ${Ufuncs.log1p(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix log1p sum: ${Ufuncs.log1p(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def powTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix pow sum: ${Ufuncs.pow(matrixlist.get(i), 2.0).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix pow sum: ${Ufuncs.pow(lmatrixlist.get(i), 2.0).sum()}")
    }
  }

  @Test
  def sigmoidTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix sigmoid sum: ${TransFuncs.sigmoid(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix sigmoid sum: ${TransFuncs.sigmoid(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def sqrtTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix sqrt sum: ${Ufuncs.sqrt(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix sqrt sum: ${Ufuncs.sqrt(lmatrixlist.get(i)).sum()}")
    }
  }

  @Test
  def softthresholdTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix softthreshold sum: ${Ufuncs.softthreshold(matrixlist.get(i), 2.0).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix softthreshold sum: ${Ufuncs.softthreshold(lmatrixlist.get(i), 2.0).sum()}")
    }
  }

  @Test
  def iaddTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix iadd matrix: ${matrixlist.get(i).iadd(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix iadd vector: ${matrixlist.get(i).iadd(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix isadd: ${matrixlist.get(i).iadd(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix iadd matrix: ${lmatrixlist.get(i).iadd(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix iadd vector: ${lmatrixlist.get(i).iadd(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def isubTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix isub matrix: ${matrixlist.get(i).isub(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix isub vector: ${matrixlist.get(i).isub(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix issub: ${matrixlist.get(i).isub(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix isub matrix: ${lmatrixlist.get(i).isub(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix isub vector: ${lmatrixlist.get(i).isub(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def imulTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix imul matrix: ${matrixlist.get(i).imul(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix imul vector: ${matrixlist.get(i).mul(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix ismul: ${matrixlist.get(i).imul(2).sum()}")
    }

    //lonhkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix imul matrix: ${lmatrixlist.get(i).imul(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix imul vector: ${lmatrixlist.get(i).mul(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
        }
      }
      println(s"matrix ismul: ${lmatrixlist.get(i).imul(2).sum()}")
    }
  }

  @Test
  def idivTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix idiv matrix: ${matrixlist.get(i).idiv(matrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix idiv vector: ${matrixlist.get(i).idiv(vectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      println(s"matrix isdiv: ${matrixlist.get(i).idiv(2).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix idiv matrix: ${lmatrixlist.get(i).idiv(lmatrixlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix idiv vector: ${lmatrixlist.get(i).idiv(lvectorlist.get(j)).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      println(s"matrix isdiv: ${lmatrixlist.get(i).idiv(2).sum()}")
    }
  }

  @Test
  def iaxpyTest() {
    (0 until matrixlist.size).foreach { i =>
      (0 until matrixlist.size).foreach { j =>
        try {
          println(s"matrix iaxpy matrix: ${matrixlist.get(i).iaxpy(matrixlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until vectorlist.size).foreach { j =>
        try {
          println(s"matrix iaxpy vector: ${matrixlist.get(i).iaxpy(vectorlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      (0 until lmatrixlist.size).foreach { j =>
        try {
          println(s"matrix iaxpy matrix: ${lmatrixlist.get(i).iaxpy(lmatrixlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
      (0 until lvectorlist.size).foreach { j =>
        try {
          println(s"matrix iaxpy vector: ${lmatrixlist.get(i).iaxpy(lvectorlist.get(j), 2.0).sum()}")
        } catch {
          case e: AngelException => {
            println(e)
          }
          case e: ArithmeticException => {
            println(e)
          }
        }
      }
    }
  }

  @Test
  def iexpTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix iexp sum: ${Ufuncs.iexp(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix iexp sum: ${Ufuncs.iexp(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def ilogTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix ilog sum: ${Ufuncs.ilog(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix ilog sum: ${Ufuncs.ilog(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def ilog1pTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix ilog1p sum: ${Ufuncs.ilog1p(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix log1p sum: ${Ufuncs.ilog1p(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def ipowTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix ipow sum: ${Ufuncs.ipow(matrixlist.get(i), 2.0).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix pow sum: ${Ufuncs.ipow(lmatrixlist.get(i), 2.0).sum()}")
    }
  }

  @Test
  def isigmoidTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix isigmoid sum: ${TransFuncs.isigmoid(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      try {
        println(s"matrix isigmoid sum: ${TransFuncs.isigmoid(lmatrixlist.get(i)).sum()}")
      } catch {
        case e: AngelException => {
          println(e)
        }
      }
    }
  }

  @Test
  def isqrtTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix isqrt sum: ${Ufuncs.isqrt(matrixlist.get(i)).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix sqrt sum: ${Ufuncs.isqrt(lmatrixlist.get(i)).sum()}")
    }
  }

  @Test
  def isoftthresholdTest() {
    (0 until matrixlist.size).foreach { i =>
      println(s"matrix isoftthreshold sum: ${Ufuncs.isoftthreshold(matrixlist.get(i), 2.0).sum()}")
    }

    //longkey
    (0 until lmatrixlist.size).foreach { i =>
      println(s"matrix softthreshold sum: ${Ufuncs.isoftthreshold(lmatrixlist.get(i), 2.0).sum()}")
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
