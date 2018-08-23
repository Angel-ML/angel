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


package com.tencent.angel.spark.ops

import scala.collection.parallel.mutable.ParArray

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType._
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapInPlace
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.{Set => SetFunc}
import com.tencent.angel.spark.models.matrix.PSMatrix

class MatrixOps {


  def pull(mat: PSMatrix): Matrix = {

    mat.rowType match {
      case T_DOUBLE_DENSE | T_DOUBLE_SPARSE => new RBIntDoubleMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[IntDoubleVector]).toArray)
      case T_FLOAT_DENSE | T_FLOAT_SPARSE => new RBIntFloatMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[IntFloatVector]).toArray)
      case T_LONG_DENSE | T_LONG_SPARSE => new RBIntLongMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[IntLongVector]).toArray)
      case T_INT_DENSE | T_INT_SPARSE => new RBIntIntMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[IntIntVector]).toArray)
      case T_DOUBLE_SPARSE_LONGKEY => new RBLongDoubleMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[LongDoubleVector]).toArray)
      case T_FLOAT_SPARSE_LONGKEY => new RBLongFloatMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[LongFloatVector]).toArray)
      case T_LONG_SPARSE_LONGKEY => new RBLongLongMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[LongLongVector]).toArray)
      case T_INT_SPARSE_LONGKEY => new RBLongIntMatrix(
        mat.id, 0, ParArray.tabulate(mat.rows)(mat.pull(_).asInstanceOf[LongIntVector]).toArray)
      case _ => throw new AngelException("type error")
    }
  }

  def random(mat: PSMatrix): Unit = {
    mat.assertValid()
    mat.psfUpdate(new Random(mat.id)).get()
  }

  def randomUniformRows(mat: PSMatrix, rows: Array[Int], min: Double, max: Double): Unit = {
    mat.assertValid()
    rows.foreach { rId =>
      mat.psfUpdate(new RandomUniform(mat.id, rId, min, max)).get()
    }
  }

  /**
    * Assign matrix diagonal with `value`
    */
  def diag(mat: PSMatrix, value: Array[Double]): Unit = {
    mat.assertValid()
    mat.assertCompatible(value)
    assert(mat.columns == mat.rows, s"when init diag matrix, " +
      s"matrix columnNum(${mat.columns}) must equal to rowNum(${mat.rows})")
    mat.psfUpdate(new Diag(mat.id, value)).get()
  }

  /**
    * Assign matrix diagonal with 1.0
    */
  def eye(mat: PSMatrix): Unit = {
    mat.assertValid()
    assert(mat.columns == mat.rows, s", " +
      s"matrix columnNum(${mat.columns}) must equal to rowNum(${mat.rows})")
    mat.psfUpdate(new Eye(mat.id)).get()

  }

  /**
    * Fill matrix with `value`
    */
  def fill(mat: PSMatrix, value: Double): Unit = {
    mat.assertValid()
    mat.psfUpdate(new FullFill(mat.id, value))
  }


  /**
    * Fill some rows in the matrix with `value`
    */

  def fill(mat: PSMatrix, rows: Array[Int], value: Double): Unit = {
    mat.assertValid()
    rows.foreach { rId =>
      mat.psfUpdate(new MapInPlace(mat.id, rId, new SetFunc(value)))
    }
  }
}
