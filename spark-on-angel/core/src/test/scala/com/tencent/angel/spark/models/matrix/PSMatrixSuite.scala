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


package com.tencent.angel.spark.models.matrix

import scala.util.Random

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, LongDoubleVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}
import PSMatrixSuite._

class PSMatrixSuite extends PSFunSuite with SharedPSContext {

  private val rows = 10
  private val cols = 10

  test("init Dense Matrix") {
    val mat = PSMatrix.dense(rows, cols)
    val result = mat.pull()

    (0 until rows).foreach { i =>
      (0 until cols).foreach { j =>
        assert(result(i)(j) == 0.0)
      }
    }

    mat.destroy()
  }

  test("Push array") {
    val mat = PSMatrix.dense(rows, cols)
    val rand = new Random()
    val array = (0 until cols).toArray.map(_ => rand.nextDouble())

    val randRow = rand.nextInt(rows)
    mat.push(VFactory.denseDoubleVector(mat.id, randRow, 0, array))

    val result = mat.pull()

    (0 until rows).foreach { i =>
      if (i == randRow) {
        assert(array.sameElements(result(i)))
      } else {
        assert(Array.fill(cols)(0.0).sameElements(result(i)))
      }
    }
    mat.destroy()
  }

  test("Pull row") {
    val mat = PSMatrix.rand(rows, cols)
    val totalMat = mat.pull()
    val rand = new Random()
    val randRow = rand.nextInt(rows)
    val oneRow = mat.pull(randRow)
    assert(oneRow.sameElements(totalMat(randRow)))
    mat.destroy()
  }

  test("increment") {
    val mat = PSMatrix.dense(rows, cols)
    val rand = new Random()
    val firstArray = (0 until cols).toArray.map(_ => rand.nextDouble())
    val secondArray = (0 until cols).toArray.map(_ => rand.nextGaussian())

    val rowId = rand.nextInt(rows)
    mat.increment(VFactory.denseDoubleVector(mat.id, rowId, 0, firstArray))
    assert(mat.pull(rowId).sameElements(firstArray))

    mat.increment(VFactory.denseDoubleVector(mat.id, rowId, 0, secondArray))
    val sum = Array.tabulate(cols)(i => firstArray(i) + secondArray(i))
    assert(mat.pull(rowId).sameElements(sum))

    mat.destroy()
  }
}

object PSMatrixSuite {

  class MatrixImplicit(mat: Matrix) {
    def apply(i: Int): Array[Double] = mat.getRow(i).asInstanceOf[IntDoubleVector].getStorage.getValues
  }

  implicit def toMatrixImplicit(mat: Matrix): MatrixImplicit = new MatrixImplicit(mat)

  class VectorImplicit(val v: Vector) {
    def +(other: Vector): Vector = Ufuncs.add(v, other)

    def +(other: LongDoubleVector): LongDoubleVector = Ufuncs.add(v, other).asInstanceOf[LongDoubleVector]

    def -(other: LongDoubleVector): LongDoubleVector = Ufuncs.sub(v, other).asInstanceOf[LongDoubleVector]

    def *(other: LongDoubleVector): LongDoubleVector = Ufuncs.mul(v, other).asInstanceOf[LongDoubleVector]

    def /(other: LongDoubleVector): LongDoubleVector = Ufuncs.div(v, other).asInstanceOf[LongDoubleVector]

    def +=(other: Vector): Unit = Ufuncs.iadd(v, other)

    def -=(other: Vector): Unit = Ufuncs.isub(v, other)

    def /=(other: Vector): Unit = Ufuncs.idiv(v, other)

    def *=(other: Vector): Unit = Ufuncs.imul(v, other)

    def sameElements(other: Array[Double]): Boolean = {
      v.getType match {
        case RowType.T_DOUBLE_DENSE =>
          v.asInstanceOf[IntDoubleVector].getStorage.getValues.sameElements(other)
        case RowType.T_DOUBLE_SPARSE_LONGKEY =>
          val storage = v.asInstanceOf[LongDoubleVector].getStorage
          val indices = storage.getIndices.sorted
          indices.min == 0 && indices.max == other.length - 1 && storage.get(indices).sameElements(other)
        case _ => throw new AngelException("not supported")
      }
    }

    def apply(col: Long): Double = v match {
      case dense: IntDoubleVector => dense.get(col.toInt)
      case sparse: LongDoubleVector => sparse.get(col)
    }
  }

  implicit def toVectorImplicit(v: Vector): VectorImplicit = new VectorImplicit(v)
}
