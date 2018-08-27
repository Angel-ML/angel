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


package com.tencent.angel.spark.models

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, LongDoubleVector, Vector}
import com.tencent.angel.ml.matrix.RowType

object CompatibleImplicit {

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

  class MatrixImplicit(mat: Matrix) {
    def apply(i: Int): Array[Double] = mat.getRow(i).asInstanceOf[IntDoubleVector].getStorage.getValues
  }

  implicit def toMatrixImplicit(mat: Matrix): MatrixImplicit = new MatrixImplicit(mat)
}