/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.optimizer2.utils.oputils

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.optimizer2.utils.oputils.TOperation._

class ScalarExpr(var alpha: Float, var top: TOperation, var isInplace: Boolean) extends Unary {

  def this(top: TOperation, isInplace: Boolean) {
    this(0.0f, top, isInplace)
  }

  override def apply(v1: DenseDoubleVector): DenseDoubleVector = {
    val restult = if (isInplace) v1 else v1.clone()
    val values = v1.getValues
    top match {
      case Add =>
        values.indices.foreach(idx => values(idx) += alpha)
      case Sub =>
        values.indices.foreach(idx => values(idx) -= alpha)
      case Mul =>
        values.indices.foreach(idx => values(idx) *= alpha)
      case Div =>
        values.indices.foreach(idx => values(idx) /= alpha)
      case Pow =>
        values.indices.foreach(idx => values(idx) = Math.pow(values(idx), alpha))
    }

    restult
  }

  override def apply(v1: SparseDoubleVector): SparseDoubleVector = {
    top match {
      case Add =>
        throw new AngelException("not impliment!")
      case Sub =>
        throw new AngelException("not impliment!")
      case _ =>
    }

    val restult = if (isInplace) v1 else v1.clone()

    val iter = restult.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
    top match {
      case Mul =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getDoubleValue * alpha)
        }
      case Div =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getDoubleValue / alpha)
        }
      case Pow =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(Math.pow(entry.getDoubleValue, alpha))
        }
    }

    restult
  }

  override def apply(v1: SparseLongKeyDoubleVector): SparseLongKeyDoubleVector = {
    top match {
      case Add =>
        throw new AngelException("not impliment!")
      case Sub =>
        throw new AngelException("not impliment!")
      case _ =>
    }

    val restult = if (isInplace) v1 else v1.clone()

    val iter = restult.getIndexToValueMap.long2DoubleEntrySet().fastIterator()
    top match {
      case Mul =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getDoubleValue * alpha)
        }
      case Div =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getDoubleValue / alpha)
        }
      case Pow =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(Math.pow(entry.getDoubleValue, alpha))
        }
    }

    restult
  }

  override def apply(v1: DenseFloatVector): DenseFloatVector = {
    val restult = if (isInplace) v1 else v1.clone()

    val values = v1.getValues
    top match {
      case Add =>
        values.indices.foreach(idx => values(idx) += alpha)
      case Sub =>
        values.indices.foreach(idx => values(idx) -= alpha)
      case Mul =>
        values.indices.foreach(idx => values(idx) *= alpha)
      case Div =>
        values.indices.foreach(idx => values(idx) /= alpha)
      case Pow =>
        values.indices.foreach(idx => values(idx) = Math.pow(values(idx), alpha).toFloat)
    }

    restult
  }

  override def apply(v1: SparseFloatVector): SparseFloatVector = {
    top match {
      case Add =>
        throw new AngelException("not impliment!")
      case Sub =>
        throw new AngelException("not impliment!")
      case _ =>
    }

    val restult = if (isInplace) v1 else v1.clone()

    val iter = restult.getIndexToValueMap.int2FloatEntrySet().fastIterator()
    top match {
      case Mul =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getFloatValue * alpha)
        }
      case Div =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getFloatValue / alpha)
        }
      case Pow =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(Math.pow(entry.getFloatValue, alpha).toFloat)
        }
    }

    restult
  }

  override def apply(v1: SparseLongKeyFloatVector): SparseLongKeyFloatVector = {
    top match {
      case Add =>
        throw new AngelException("not impliment!")
      case Sub =>
        throw new AngelException("not impliment!")
      case _ =>
    }

    val restult = if (isInplace) v1 else v1.clone()

    val iter = restult.getIndexToValueMap.long2FloatEntrySet().fastIterator()
    top match {
      case Mul =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getFloatValue * alpha)
        }
      case Div =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(entry.getFloatValue / alpha)
        }
      case Pow =>
        while (iter.hasNext) {
          val entry = iter.next()
          entry.setValue(Math.pow(entry.getFloatValue, alpha).toFloat)
        }
    }

    restult
  }
}
