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

import com.tencent.angel.ml.math.matrix._
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.math.{TMatrix, TUpdate, TVector}


import scala.collection.JavaConversions._

object OptUtils {

  def clear(base: util.HashMap[String, TUpdate]): Unit = {
    base.foreach { case (_, basemv: TUpdate) =>
      basemv match {
        case bm: DenseDoubleMatrix =>
          (0 until bm.getRowNum).foreach { idx =>
            bm.getRow(idx).clear()
          }
        case bm: DenseFloatMatrix =>
          (0 until bm.getRowNum).foreach { idx =>
            bm.getRow(idx).clear()
          }
        case bm: SparseDoubleMatrix =>
          (0 until bm.getRowNum).foreach { idx =>
            bm.getRow(idx).clear()
          }
        case bm: SparseFloatMatrix =>
          (0 until bm.getRowNum).foreach { idx =>
            bm.getRow(idx).clear()
          }
        case bm: SparseDoubleLongKeyMatrix =>
          (0 until bm.getRowNum).foreach { idx =>
            bm.getRow(idx).clear()
          }
        case bv: DenseDoubleVector =>
          bv.clear()
        case bv: DenseFloatVector =>
          bv.clear()
        case bv: SparseDoubleVector =>
          bv.clear()
        case bv: SparseFloatVector =>
          bv.clear()
        case bv: SparseLongKeyDoubleVector =>
          bv.clear()
      }
    }
  }

  def clone(base: util.HashMap[String, TUpdate]): util.HashMap[String, TUpdate] = {
    val restlt = new util.HashMap[String, TUpdate]()

    base.foreach {
      case (name: String, bm: DenseDoubleMatrix) =>
        val mat = new DenseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          mat.setRow(idx, bm.getRow(idx).clone())
        }
        restlt.put(name, mat)
      case (name: String, bm: DenseFloatMatrix) =>
        val mat = new DenseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          mat.setRow(idx, bm.getRow(idx).clone().asInstanceOf[DenseFloatVector])
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseDoubleMatrix) =>
        val mat = new SparseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          mat.setRow(idx, bm.getRow(idx).clone().asInstanceOf[SparseDoubleVector])
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseFloatMatrix) =>
        val mat = new SparseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          mat.setRow(idx, bm.getRow(idx).clone().asInstanceOf[SparseFloatVector])
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseDoubleLongKeyMatrix) =>
        val mat = new SparseDoubleLongKeyMatrix(bm.getRowNum, bm.getColNum)
        (0 until bm.getRowNum).foreach { idx =>
          mat.setRow(idx, bm.getRow(idx).clone().asInstanceOf[SparseLongKeyDoubleVector])
        }
        restlt.put(name, mat)
      case (name: String, bv: DenseDoubleVector) =>
        restlt.put(name, bv.clone())
      case (name: String, bv: DenseFloatVector) =>
        restlt.put(name, bv.clone())
      case (name: String, bv: SparseDoubleVector) =>
        restlt.put(name, bv.clone())
      case (name: String, bv: SparseFloatVector) =>
        restlt.put(name, bv.clone())
      case (name: String, bv: SparseLongKeyDoubleVector) =>
        restlt.put(name, bv.clone())
    }

    restlt
  }

  def emptyLike(base: util.HashMap[String, TUpdate]): util.HashMap[String, TUpdate] = {
    val restlt = new util.HashMap[String, TUpdate]()
    base.foreach {
      case (name: String, bm: DenseDoubleMatrix) =>
        val mat = new DenseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
        restlt.put(name, mat)
      case (name: String, bm: DenseFloatMatrix) =>
        val mat = new DenseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          val row = bm.getRow(idx)
          mat.setRow(idx, new DenseFloatVector(row.getDimension))
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseDoubleMatrix) =>
        val mat = new SparseDoubleMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          val row = bm.getRow(idx)
          mat.setRow(idx, new SparseDoubleVector(row.getDimension, (row.size() / 0.75).toInt))
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseFloatMatrix) =>
        val mat = new SparseFloatMatrix(bm.getRowNum, bm.getColNum.toInt)
        (0 until bm.getRowNum).foreach { idx =>
          val row = bm.getRow(idx)
          mat.setRow(idx, new SparseFloatVector(row.getDimension, (row.size() / 0.75).toInt))
        }
        restlt.put(name, mat)
      case (name: String, bm: SparseDoubleLongKeyMatrix) =>
        val mat = new SparseDoubleLongKeyMatrix(bm.getRowNum, bm.getColNum)
        (0 until bm.getRowNum).foreach { idx =>
          val row = bm.getRow(idx)
          mat.setRow(idx, new SparseLongKeyDoubleVector(row.getLongDim, (row.size() / 0.75).toInt))
        }
        restlt.put(name, mat)
      case (name: String, bv: DenseDoubleVector) =>
        restlt.put(name, new DenseDoubleVector(bv.getDimension))
      case (name: String, bv: DenseFloatVector) =>
        restlt.put(name, new DenseFloatVector(bv.getDimension))
      case (name: String, bv: SparseDoubleVector) =>
        restlt.put(name, new SparseDoubleVector(bv.getDimension, (bv.size() / 0.75).toInt))
      case (name: String, bv: SparseFloatVector) =>
        restlt.put(name, new SparseFloatVector(bv.getDimension, (bv.size() / 0.75).toInt))
      case (name: String, bv: SparseLongKeyDoubleVector) =>
        restlt.put(name, new SparseLongKeyDoubleVector(bv.getLongDim, (bv.size() / 0.75).toInt))

    }

    restlt
  }

  // a[i] = a[i] + alpha * b[i]
  def iaxpy(base: util.HashMap[String, TUpdate], delta: util.HashMap[String, TUpdate], alpha: Double): Unit = {
    val alphaMap = base.map { case (name: String, _) => name -> alpha }.toMap
    val oneMap = base.map { case (name: String, _) => name -> 1.0 }.toMap
    ilinear(base, oneMap, delta, alphaMap)
  }

  def iaxpy(base: util.HashMap[String, TUpdate], delta: util.HashMap[String, TUpdate], alpha: Map[String, Double]): Unit = {
    val oneMap = base.map { case (name: String, _) => name -> 1.0 }.toMap
    ilinear(base, oneMap, delta, alpha)
  }

  // c[i] = a[i] + alpha * b[i]
  def axpy(base: util.HashMap[String, TUpdate], delta: util.HashMap[String, TUpdate], alpha: Double): util.HashMap[String, TUpdate] = {
    val alphaMap = base.map { case (name: String, _) => name -> alpha }.toMap
    val oneMap = base.map { case (name: String, _) => name -> 1.0 }.toMap
    linear(base, oneMap, delta, alphaMap)
  }

  def axpy(base: util.HashMap[String, TUpdate], delta: util.HashMap[String, TUpdate], alpha: Map[String, Double]): util.HashMap[String, TUpdate] = {
    val oneMap = base.map { case (name: String, _) => name -> 1.0 }.toMap
    linear(base, oneMap, delta, alpha)
  }

  // a[i] = aplha * a[i] + beta * b[i]
  def ilinear(base: util.HashMap[String, TUpdate], alpha: Double,
              grad: util.HashMap[String, TUpdate], beta: Double): Unit = {
    val alphaMap = base.map { case (name: String, _) => name -> alpha }.toMap
    val betaMap = grad.map { case (name: String, _) => name -> beta }.toMap
    ilinear(base, alphaMap, grad, betaMap)
  }

  def ilinear(base: util.HashMap[String, TUpdate], alpha: Map[String, Double],
              grad: util.HashMap[String, TUpdate], beta: Map[String, Double]): Unit = {
    base.foreach { case (name: String, basemv: TUpdate) =>
      val gradmv = grad.get(name)
      val (a, b) = (alpha(name), beta(name))
      (basemv, gradmv) match {
        case (bv: DenseDoubleVector, gv: DenseDoubleVector) =>
          linear(bv, a, gv, b, isInplace = true)
        case (bv: DenseDoubleVector, gv: SparseDoubleVector) =>
          linear(bv, a, gv, b, isInplace = true)
        case (bv: SparseDoubleVector, gv: SparseDoubleVector) =>
          linear(bv, a, gv, b, isInplace = true)
        case (bv: SparseLongKeyDoubleVector, gv: SparseLongKeyDoubleVector) =>
          linear(bv, a, gv, b, isInplace = true)
        case (bv: DenseFloatVector, gv: DenseFloatVector) =>
          linear(bv, a.toFloat, gv, b.toFloat, isInplace = true)
        case (bv: DenseFloatVector, gv: SparseFloatVector) =>
          linear(bv, a.toFloat, gv, b.toFloat, isInplace = true)
        case (bv: SparseFloatVector, gv: SparseFloatVector) =>
          linear(bv, a.toFloat, gv, b.toFloat, isInplace = true)
        case (bm: DenseDoubleMatrix, gm: DenseDoubleMatrix) =>
          linear(bm, a, gm, b, isInplace = true)
        case (bm: DenseDoubleMatrix, gm: SparseDoubleMatrix) =>
          linear(bm, a, gm, b, isInplace = true)
        case (bm: SparseDoubleLongKeyMatrix, gm: SparseDoubleLongKeyMatrix) =>
          linear(bm, a.toFloat, gm, b.toFloat, isInplace = true)
        case (bm: DenseFloatMatrix, gm: DenseFloatMatrix) =>
          linear(bm, a.toFloat, gm, b.toFloat, isInplace = true)
        case (bm: DenseFloatMatrix, gm: SparseFloatMatrix) =>
          linear(bm, a.toFloat, gm, b.toFloat, isInplace = true)
        case (bm: SparseDoubleMatrix, gm: SparseDoubleMatrix) =>
          linear(bm, a, gm, b, isInplace = true)
        case (bm: SparseFloatMatrix, gm: SparseFloatMatrix) =>
          linear(bm, a.toFloat, gm, b.toFloat, isInplace = true)
      }
    }
  }

  // c[i] = aplha * a[i] + beta * b[i]
  def linear(base: util.HashMap[String, TUpdate], alpha: Double,
             grad: util.HashMap[String, TUpdate], beta: Double): util.HashMap[String, TUpdate] = {
    val alphaMap = base.map { case (name: String, _) => name -> alpha }.toMap
    val betaMap = grad.map { case (name: String, _) => name -> beta }.toMap
    linear(base, alphaMap, grad, betaMap)
  }

  def linear(base: util.HashMap[String, TUpdate], alpha: Map[String, Double],
             grad: util.HashMap[String, TUpdate], beta: Map[String, Double]): util.HashMap[String, TUpdate] = {
    val result = new util.HashMap[String, TUpdate]()

    base.foreach { case (name: String, basemv: TUpdate) =>
      val gradmv = grad.get(name)
      val (a, b) = (alpha(name), beta(name))
      (basemv, gradmv) match {
        case (bv: DenseDoubleVector, gv: DenseDoubleVector) =>
          result.put(name, linear(bv, a, gv, b, isInplace = false))
        case (bv: DenseDoubleVector, gv: SparseDoubleVector) =>
          result.put(name, linear(bv, a, gv, b, isInplace = false))
        case (bv: DenseFloatVector, gv: DenseFloatVector) =>
          result.put(name, linear(bv, a.toFloat, gv, b.toFloat, isInplace = false))
        case (bv: DenseFloatVector, gv: SparseFloatVector) =>
          result.put(name, linear(bv, a.toFloat, gv, b.toFloat, isInplace = false))
        case (bv: SparseDoubleVector, gv: SparseDoubleVector) =>
          result.put(name, linear(bv, a, gv, b, isInplace = false))
        case (bv: SparseFloatVector, gv: SparseFloatVector) =>
          result.put(name, linear(bv, a.toFloat, gv, b.toFloat, isInplace = false))
        case (bv: SparseLongKeyDoubleVector, gv: SparseLongKeyDoubleVector) =>
          result.put(name, linear(bv, a, gv, b, isInplace = false))
        case (bm: SparseDoubleLongKeyMatrix, gm: SparseDoubleLongKeyMatrix) =>
          result.put(name, linear(bm, a.toFloat, gm, b.toFloat, isInplace = false))
        case (bm: DenseDoubleMatrix, gm: DenseDoubleMatrix) =>
          result.put(name, linear(bm, a, gm, b, isInplace = false))
        case (bm: DenseDoubleMatrix, gm: SparseDoubleMatrix) =>
          result.put(name, linear(bm, a, gm, b, isInplace = false))
        case (bm: DenseFloatMatrix, gm: DenseFloatMatrix) =>
          result.put(name, linear(bm, a.toFloat, gm, b.toFloat, isInplace = false))
        case (bm: DenseFloatMatrix, gm: SparseFloatMatrix) =>
          result.put(name, linear(bm, a.toFloat, gm, b.toFloat, isInplace = false))
        case (bm: SparseDoubleMatrix, gm: SparseDoubleMatrix) =>
          result.put(name, linear(bm, a, gm, b, isInplace = false))
        case (bm: SparseFloatMatrix, gm: SparseFloatMatrix) =>
          result.put(name, linear(bm, a.toFloat, gm, b.toFloat, isInplace = false))
      }
    }

    result
  }


  private def linear(base: DenseDoubleVector, alpha: Double, grad: DenseDoubleVector, beta: Double, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: DenseDoubleVector, alpha: Double, grad: SparseDoubleVector, beta: Double, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: DenseFloatVector, alpha: Float, grad: DenseFloatVector, beta: Float, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: DenseFloatVector, alpha: Float, grad: SparseFloatVector, beta: Float, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: SparseDoubleVector, alpha: Double, grad: SparseDoubleVector, beta: Double, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: SparseFloatVector, alpha: Float, grad: SparseFloatVector, beta: Float, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: SparseLongKeyDoubleVector, alpha: Double, grad: SparseLongKeyDoubleVector, beta: Double, isInplace: Boolean): TVector = {
    if (isInplace) {
      if (alpha == 1.0 && beta == 1.0) {
        base.plusBy(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plusBy(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.timesBy(alpha).plusBy(grad)
      } else {
        base.timesBy(alpha).plusBy(grad, beta)
      }
    } else {
      val result = if (alpha == 1.0 && beta == 1.0) {
        base.plus(grad)
      } else if (beta == 1.0 && beta != 1.0) {
        base.plus(grad, beta)
      } else if (beta != 1.0 && beta == 1.0) {
        base.times(alpha).plus(grad)
      } else {
        base.times(alpha).plus(grad, beta)
      }

      result.setClock(base.getClock)
      result.setMatrixId(base.getMatrixId)
      result.setRowId(base.getRowId)

      result
    }
  }

  private def linear(base: DenseDoubleMatrix, alpha: Double, grad: DenseDoubleMatrix, beta: Double, isInplace: Boolean): TMatrix[DenseDoubleVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new DenseDoubleMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[DenseDoubleVector])
      }
      result
    }
  }

  private def linear(base: DenseDoubleMatrix, alpha: Double, grad: SparseDoubleMatrix, beta: Double, isInplace: Boolean): TMatrix[DenseDoubleVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new DenseDoubleMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[DenseDoubleVector])
      }
      result
    }
  }

  private def linear(base: DenseFloatMatrix, alpha: Float, grad: DenseFloatMatrix, beta: Float, isInplace: Boolean): TMatrix[DenseFloatVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new DenseFloatMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[DenseFloatVector])
      }
      result
    }
  }

  private def linear(base: DenseFloatMatrix, alpha: Float, grad: SparseFloatMatrix, beta: Float, isInplace: Boolean): TMatrix[DenseFloatVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new DenseFloatMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[DenseFloatVector])
      }
      result
    }
  }

  private def linear(base: SparseDoubleMatrix, alpha: Double, grad: SparseDoubleMatrix, beta: Double, isInplace: Boolean): TMatrix[SparseDoubleVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new SparseDoubleMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[SparseDoubleVector])
      }
      result
    }
  }

  private def linear(base: SparseFloatMatrix, alpha: Float, grad: SparseFloatMatrix, beta: Float, isInplace: Boolean): TMatrix[SparseFloatVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new SparseFloatMatrix(base.getRowNum, base.getColNum.toInt)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[SparseFloatVector])
      }
      result
    }
  }

  private def linear(base: SparseDoubleLongKeyMatrix, alpha: Double, grad: SparseDoubleLongKeyMatrix, beta: Double, isInplace: Boolean): TMatrix[SparseLongKeyDoubleVector] = {
    if (isInplace) {
      (0 until base.getRowNum).foreach { idx =>
        linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace)
      }
      base
    } else {
      val result = new SparseDoubleLongKeyMatrix(base.getRowNum, base.getColNum)
      result.setMatrixId(base.getMatrixId)
      (0 until base.getRowNum).foreach { idx =>
        result.setRow(idx, linear(base.getRow(idx), alpha, grad.getRow(idx), beta, isInplace).asInstanceOf[SparseLongKeyDoubleVector])
      }
      result
    }
  }

}
