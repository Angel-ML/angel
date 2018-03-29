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

package com.tencent.angel.ml.optimizer2.utils

import java.util

import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.math.matrix._
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.optimizer2.utils.oputils.{Binary, ScalarExpr, Ternary, Unary}

import scala.collection.JavaConversions._


object ExecuteUtils {
  def executeScalar(base: util.HashMap[String, TUpdate], op: ScalarExpr): util.HashMap[String, TUpdate] = {
    val alphaMap = base.map { case (name, _) => name -> op.alpha.toDouble }.toMap
    executeScalar(base, alphaMap, op)
  }

  def executeScalar(base: util.HashMap[String, TUpdate], alpha: Map[String, Double], op: ScalarExpr): util.HashMap[String, TUpdate] = {
    val restult = if (op.isInplace) base else new util.HashMap[String, TUpdate]

    base.foreach {
      case (name: String, bv: DenseDoubleVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bv: DenseFloatVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bv: SparseDoubleVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bv: SparseLongKeyDoubleVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bv: SparseFloatVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bv: SparseLongKeyFloatVector) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) op(bv) else restult.put(name, op(bv))
      case (name: String, bm: DenseDoubleMatrix) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) {
          bm.getVectors.foreach(vect => op(vect))
        } else {
          val temp = new DenseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
          bm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            temp.setRow(vidx, op(vect))
          }
          restult.put(name, temp)
        }
      case (name: String, bm: DenseFloatMatrix) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) {
          bm.getVectors.foreach(vect => op(vect))
        } else {
          val temp = new DenseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
          bm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            temp.setRow(vidx, op(vect))
          }
          restult.put(name, temp)
        }
      case (name: String, bm: SparseDoubleMatrix) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) {
          bm.getVectors.foreach(vect => op(vect))
        } else {
          val temp = new SparseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
          bm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            temp.setRow(vidx, op(vect))
          }
          restult.put(name, temp)
        }
      case (name: String, bm: SparseDoubleLongKeyMatrix) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) {
          bm.getVectors.foreach(vect => op(vect))
        } else {
          val temp = new SparseDoubleLongKeyMatrix(bm.getRowNum, bm.getColNum)
          bm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            temp.setRow(vidx, op(vect))
          }
          restult.put(name, temp)
        }
      case (name: String, bm: SparseFloatMatrix) =>
        op.alpha = alpha(name).toFloat
        if (op.isInplace) {
          bm.getVectors.foreach(vect => op(vect))
        } else {
          val temp = new SparseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
          bm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            temp.setRow(vidx, op(vect))
          }
          restult.put(name, temp)
        }
      case _ =>

    }

    restult
  }

  def executeUnary(grad: util.HashMap[String, TUpdate], flag: util.HashMap[String, Boolean], top: Unary, fop: Unary): util.HashMap[String, TUpdate] = {
    val result = new util.HashMap[String, TUpdate]

    grad.foreach { case (name: String, gradmv: TUpdate) =>
      gradmv match {
        case gv: DenseDoubleVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gv: DenseFloatVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gv: SparseDoubleVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gv: SparseFloatVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gv: SparseLongKeyDoubleVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gv: SparseLongKeyFloatVector =>
          if (flag(name)) {
            result.put(name, top(gv))
          } else {
            result.put(name, fop(gv))
          }
        case gm: DenseDoubleMatrix =>
          val temp = new DenseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect))
            } else {
              temp.setRow(vidx, fop(vect))
            }
          }
          result.put(name, temp)
        case gm: DenseFloatMatrix =>
          val temp = new DenseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect))
            } else {
              temp.setRow(vidx, fop(vect))
            }
          }
          result.put(name, temp)
        case gm: SparseDoubleMatrix =>
          val temp = new SparseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect))
            } else {
              temp.setRow(vidx, fop(vect))
            }
          }
          result.put(name, temp)
        case gm: SparseDoubleLongKeyMatrix =>
          val temp = new SparseDoubleLongKeyMatrix(gm.getRowNum, gm.getColNum)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect))
            } else {
              temp.setRow(vidx, fop(vect))
            }
          }
          result.put(name, temp)
        case gm: SparseFloatMatrix =>
          val temp = new SparseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect))
            } else {
              temp.setRow(vidx, fop(vect))
            }
          }
          result.put(name, temp)
      }
    }

    result
  }

  def executeBinary(grad: util.HashMap[String, TUpdate], base: util.HashMap[String, TUpdate],
                    flag: util.HashMap[String, Boolean], top: Binary, fop: Binary): util.HashMap[String, TUpdate] = {
    val result = new util.HashMap[String, TUpdate]

    grad.foreach { case (name: String, gradmv: TUpdate) =>
      val basemv = base.get(name)
      (gradmv, basemv) match {
        case (gv: DenseDoubleVector, bv: TDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gv: DenseFloatVector, bv: TFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gv: SparseDoubleVector, bv: TDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gv: SparseFloatVector, bv: TFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gv: SparseLongKeyDoubleVector, bv: TLongDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gv: SparseLongKeyFloatVector, bv: TLongFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, bv))
          } else {
            result.put(name, fop(gv, bv))
          }
        case (gm: DenseDoubleMatrix, bm: TDoubleMatrix[_]) =>
          val temp = new DenseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, bm.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, bm.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: DenseFloatMatrix, bm: TFloatMatrix[_]) =>
          val temp = new DenseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, bm.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, bm.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseDoubleMatrix, bm: TDoubleMatrix[_]) =>
          val temp = new SparseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, bm.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, bm.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseDoubleLongKeyMatrix, bm: DoubleLongKeyMatrix[_]) =>
          val temp = new SparseDoubleLongKeyMatrix(gm.getRowNum, gm.getColNum)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, bm.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, bm.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseFloatMatrix, bm: TFloatMatrix[_]) =>
          val temp = new SparseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, bm.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, bm.getRow(vidx)))
            }
          }
          result.put(name, temp)
      }
    }

    result
  }

  def executeTernary(grad: util.HashMap[String, TUpdate], base1: util.HashMap[String, TUpdate],
                     base2: util.HashMap[String, TUpdate], flag: util.HashMap[String, Boolean], top: Ternary, fop: Ternary): util.HashMap[String, TUpdate] = {
    val result = new util.HashMap[String, TUpdate]

    grad.foreach { case (name: String, gradmv: TUpdate) =>
      val (b1mv, b2mv) = (base1(name), base2(name))
      (gradmv, b1mv, b2mv) match {
        case (gv: DenseDoubleVector, b1v: TDoubleVector, b2v: TDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gv: DenseFloatVector, b1v: TFloatVector, b2v: TFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gv: SparseDoubleVector, b1v: TDoubleVector, b2v: TDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gv: SparseLongKeyDoubleVector, b1v: TLongDoubleVector, b2v: TLongDoubleVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gv: SparseFloatVector, b1v: TFloatVector, b2v: TFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gv: SparseLongKeyFloatVector, b1v: TLongFloatVector, b2v: TLongFloatVector) =>
          if (flag(name)) {
            result.put(name, top(gv, b1v, b2v))
          } else {
            result.put(name, fop(gv, b1v, b2v))
          }
        case (gm: DenseDoubleMatrix, b1m: TDoubleMatrix[_], b2m: TDoubleMatrix[_]) =>
          val temp = new DenseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: DenseFloatMatrix, b1m: TFloatMatrix[_], b2m: TFloatMatrix[_]) =>
          val temp = new DenseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseDoubleMatrix, b1m: TDoubleMatrix[_], b2m: TDoubleMatrix[_]) =>
          val temp = new SparseDoubleMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseDoubleLongKeyMatrix, b1m: DoubleLongKeyMatrix[_], b2m: DoubleLongKeyMatrix[_]) =>
          val temp = new SparseDoubleLongKeyMatrix(gm.getRowNum, gm.getColNum)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            }
          }
          result.put(name, temp)
        case (gm: SparseFloatMatrix, b1m: TFloatMatrix[_], b2m: TFloatMatrix[_]) =>
          val temp = new SparseFloatMatrix(gm.getRowNum, gm.getColNum.toInt)
          temp.setMatrixId(gm.getMatrixId)
          gm.getVectors.zipWithIndex.foreach { case (vect, vidx) =>
            if (flag(name)) {
              temp.setRow(vidx, top(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            } else {
              temp.setRow(vidx, fop(vect, b1m.getRow(vidx), b2m.getRow(vidx)))
            }
          }
          result.put(name, temp)
      }
    }

    result
  }

}
