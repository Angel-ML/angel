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
package com.tencent.angel.spark.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, IntIntVector, IntLongVector, LongDoubleVector, LongFloatVector, LongIntVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.RowType._

object PSMatrixUtils {
  def createFromVectorArray(matrixId: Int, rowType: RowType, rows: Array[Vector]): Matrix = {
    rowType match {
      case T_DOUBLE_DENSE | T_DOUBLE_SPARSE => new RBIntDoubleMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[IntDoubleVector]))
      case T_FLOAT_DENSE | T_FLOAT_SPARSE => new RBIntFloatMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[IntFloatVector]))
      case T_LONG_DENSE | T_LONG_SPARSE => new RBIntLongMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[IntLongVector]))
      case T_INT_DENSE | T_INT_SPARSE => new RBIntIntMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[IntIntVector]))
      case T_DOUBLE_SPARSE_LONGKEY => new RBLongDoubleMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[LongDoubleVector]))
      case T_FLOAT_SPARSE_LONGKEY => new RBLongFloatMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[LongFloatVector]))
      case T_LONG_SPARSE_LONGKEY => new RBLongLongMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[LongLongVector]))
      case T_INT_SPARSE_LONGKEY => new RBLongIntMatrix(
        matrixId, 0, rows.map(_.asInstanceOf[LongIntVector]))
      case _ => throw new AngelException("type error")
    }
  }
}
