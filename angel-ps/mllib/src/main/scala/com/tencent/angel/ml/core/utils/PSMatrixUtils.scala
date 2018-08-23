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


package com.tencent.angel.ml.core.utils

import java.util.{ArrayList => JArrayList, List => JList}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.matrix.psf.get.getrows.{GetRows, GetRowsParam, GetRowsResult}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.server.data.request.UpdateOp
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.psagent.PSAgentContext


object PSMatrixUtils {

  def getMatrixId(name: String): Int = {
    val master = PSAgentContext.get().getMasterClient
    val meta = master.getMatrix(name)
    meta.getMatrixContext.getMatrixId
  }

  def createPSMatrixCtx(name: String, numRows: Int, numCols: Long, rowType: RowType): MatrixContext = {
    val matrix = new MatrixContext(name, numRows, numCols)
    matrix.setPartitionerClass(classOf[ColumnRangePartitioner])
    matrix.setRowType(rowType)
    matrix
  }

  def createPSMatrix(ctxs: Iterable[MatrixContext]): Unit = {
    val master = PSAgentContext.get().getMasterClient
    val list = new JArrayList[MatrixContext]()
    ctxs.foreach(ctx => list.add(ctx))
    master.createMatrices(list, Long.MaxValue)
  }

  def createPSMatrix(ctx: MatrixContext): Unit = {
    val master = PSAgentContext.get().getMasterClient
    master.createMatrix(ctx, Long.MaxValue)
  }

  def getRow(matrixId: Int, rowId: Int): Vector = {
    PSAgentContext.get.getUserRequestAdapter.getRow(matrixId, rowId, 0)
  }

  def getRowWithIndex(matrixId: Int, rowId: Int, index: Vector): Vector = {
    val futureVector = index match {
      case v: IntIntVector if v.isDense =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowId, v.getStorage.getValues)
      case v: IntDummyVector => v.getIndices
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowId, v.getIndices)
      case v: IntLongVector if v.isDense =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowId, v.getStorage.getValues)
      case v: LongDummyVector =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowId, v.getIndices)
    }

    futureVector.get()
  }

  def getRowsWithIndex(matrixId: Int, rowIds: Array[Int], index: Vector): Array[Vector] = {
    val futureVector = index match {
      case v: IntIntVector if v.isDense =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowIds, v.getStorage.getValues)
      case v: IntDummyVector => v.getIndices
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowIds, v.getIndices)
      case v: IntLongVector if v.isDense =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowIds, v.getStorage.getValues)
      case v: LongDummyVector =>
        PSAgentContext.get.getUserRequestAdapter.get(matrixId, rowIds, v.getIndices)
    }

    futureVector.get()
  }

  def getRowAsMatrix(matrixId: Int, rowId: Int, matRows: Int, matCols: Int): Matrix = {
    val vector = getRow(matrixId, rowId)

    assert(vector.isDense)
    vector.getStorage match {
      case s: DoubleVectorStorage =>
        MFactory.denseDoubleMatrix(vector.getMatrixId, vector.getClock, matRows, matCols, s.getValues)
      case s: FloatVectorStorage =>
        MFactory.denseFloatMatrix(vector.getMatrixId, vector.getClock, matRows, matCols, s.getValues)
      case _ => throw new AngelException("Only Double and Float are supported!")
    }
  }

  def getMatrix(matrixId: Int, startRowId: Int, endRowId: Int): Matrix = {
    val idxArr = (startRowId until endRowId).toArray
    val param = new GetRowsParam(matrixId, idxArr)
    val func = new GetRows(param)
    val vectorMap = PSAgentContext.get.getUserRequestAdapter.get(func)
      .asInstanceOf[GetRowsResult].getRows

    val vectors = idxArr.map { rowId => vectorMap.get(rowId) }
    vectors.head match {
      case _: CompIntDoubleVector =>
        if (vectors.forall(_.isDense)) {
          var data: Array[Double] = null

          vectors.zipWithIndex.foreach { case (vec, row) =>
            val dim = vec.asInstanceOf[CompIntDoubleVector].getDim

            if (data == null) {
              data = Array[Double](dim * vectors.length)
            }

            vec.asInstanceOf[CompIntDoubleVector].getPartitions.zipWithIndex.foreach { case (part, idx) =>
              val subDim = part.getDim
              Array.copy(part.getStorage.getValues, 0, data, row * dim + idx * subDim, subDim)
            }
          }

          MFactory.denseDoubleMatrix(vectors.length, data.length / vectors.length, data)
        } else {
          MFactory.rbCompIntDoubleMatrix(vectors.asInstanceOf[Array[CompIntDoubleVector]])
        }
      case _: CompIntFloatVector =>
        if (vectors.forall(_.isDense)) {
          var data: Array[Float] = null

          vectors.zipWithIndex.foreach { case (vec, row) =>
            val dim = vec.asInstanceOf[CompIntDoubleVector].getDim

            if (data == null) {
              data = Array[Float](dim * vectors.length)
            }

            vec.asInstanceOf[CompIntFloatVector].getPartitions.zipWithIndex.foreach { case (part, idx) =>
              val subDim = part.getDim
              Array.copy(part.getStorage.getValues, 0, data, row * dim + idx * subDim, subDim)
            }
          }

          MFactory.denseFloatMatrix(vectors.length, data.length / vectors.length, data)
        } else {
          MFactory.rbCompIntFloatMatrix(vectors.asInstanceOf[Array[CompIntFloatVector]])
        }
      case _: CompLongDoubleVector =>
        MFactory.rbCompLongDoubleMatrix(vectors.asInstanceOf[Array[CompLongDoubleVector]])
      case _: CompLongFloatVector =>
        MFactory.rbCompLongFloatMatrix(vectors.asInstanceOf[Array[CompLongFloatVector]])
      case v: IntDoubleVector =>
        if (v.isDense) {
          var data: Array[Double] = null

          vectors.zipWithIndex.foreach { case (vec, row) =>
            val valArr = vec.asInstanceOf[IntDoubleVector].getStorage.getValues
            if (data == null) {
              data = Array[Double](valArr.length * vectors.length)
            }

            Array.copy(valArr, 0, data, row * valArr.length, valArr.length)
          }

          MFactory.denseDoubleMatrix(vectors.length, data.length / vectors.length, data)
        } else {
          MFactory.rbIntDoubleMatrix(vectors.asInstanceOf[Array[IntDoubleVector]])
        }
      case v: IntFloatVector =>
        if (v.isDense) {
          var data: Array[Float] = null

          vectors.zipWithIndex.foreach { case (vec, row) =>
            val valArr = vec.asInstanceOf[IntFloatVector].getStorage.getValues
            if (data == null) {
              data = Array[Float](valArr.length * vectors.length)
            }

            Array.copy(valArr, 0, data, row * valArr.length, valArr.length)
          }

          MFactory.denseFloatMatrix(vectors.length, data.length / vectors.length, data)
        } else {
          MFactory.rbIntFloatMatrix(vectors.asInstanceOf[Array[IntFloatVector]])
        }
      case _: LongDoubleVector =>
        MFactory.rbLongDoubleMatrix(vectors.asInstanceOf[Array[LongDoubleVector]])
      case _: LongFloatVector =>
        MFactory.rbLongFloatMatrix(vectors.asInstanceOf[Array[LongFloatVector]])
    }
  }

  def getMatrixWithIndex(matrixId: Int, startRowId: Int, endRowId: Int, index: Vector): Matrix = {
    val vectors = getRowsWithIndex(matrixId, (startRowId until endRowId).toArray, index)

    vectors.head match {
      case _: CompIntDoubleVector =>
        MFactory.rbCompIntDoubleMatrix(vectors.map(_.asInstanceOf[CompIntDoubleVector]))
      case _: CompIntFloatVector =>
        MFactory.rbCompIntFloatMatrix(vectors.map(_.asInstanceOf[CompIntFloatVector]))
      case _: CompIntLongVector =>
        MFactory.rbCompIntLongMatrix(vectors.map(_.asInstanceOf[CompIntLongVector]))
      case _: CompIntIntVector =>
        MFactory.rbCompIntIntMatrix(vectors.map(_.asInstanceOf[CompIntIntVector]))
      case _: CompLongDoubleVector =>
        MFactory.rbCompLongDoubleMatrix(vectors.map(_.asInstanceOf[CompLongDoubleVector]))
      case _: CompLongFloatVector =>
        MFactory.rbCompLongFloatMatrix(vectors.map(_.asInstanceOf[CompLongFloatVector]))
      case _: CompLongLongVector =>
        MFactory.rbCompLongLongMatrix(vectors.map(_.asInstanceOf[CompLongLongVector]))
      case _: CompLongIntVector =>
        MFactory.rbCompLongIntMatrix(vectors.map(_.asInstanceOf[CompLongIntVector]))
      case _: IntDoubleVector =>
        MFactory.rbIntDoubleMatrix(vectors.map(_.asInstanceOf[IntDoubleVector]))
      case _: IntFloatVector =>
        MFactory.rbIntFloatMatrix(vectors.map(_.asInstanceOf[IntFloatVector]))
      case _: IntLongVector =>
        MFactory.rbIntLongMatrix(vectors.map(_.asInstanceOf[IntLongVector]))
      case _: IntIntVector =>
        MFactory.rbIntIntMatrix(vectors.map(_.asInstanceOf[IntIntVector]))
      case _: LongDoubleVector =>
        MFactory.rbLongDoubleMatrix(vectors.map(_.asInstanceOf[LongDoubleVector]))
      case _: LongFloatVector =>
        MFactory.rbLongFloatMatrix(vectors.map(_.asInstanceOf[LongFloatVector]))
      case _: LongLongVector =>
        MFactory.rbLongLongMatrix(vectors.map(_.asInstanceOf[LongLongVector]))
      case _: LongIntVector =>
        MFactory.rbLongIntMatrix(vectors.map(_.asInstanceOf[LongIntVector]))
    }
  }

  def incrementRowByMatrix(matrixId: Int, rowId: Int, mat: Matrix): Unit = {
    val vector = mat match {
      case m: BlasDoubleMatrix =>
        VFactory.denseDoubleVector(mat.getMatrixId, rowId, mat.getClock, m.getData)
      case m: BlasFloatMatrix =>
        VFactory.denseFloatVector(mat.getMatrixId, rowId, mat.getClock, m.getData)
      case _ => throw new AngelException("Only Double and Float are supported!")
    }

    incrementRow(matrixId, rowId, vector)
  }

  def incrementRow(matrixId: Int, rowId: Int, vector: Vector): Unit = {
    PSAgentContext.get().getUserRequestAdapter.update(matrixId, rowId, vector, UpdateOp.PLUS).get()
  }

  def incrementRows(matrixId: Int, rowIds: Array[Int], vectors: Array[Vector]): Unit = {
    PSAgentContext.get().getUserRequestAdapter.update(matrixId, rowIds, vectors, UpdateOp.PLUS).get()
  }
}
