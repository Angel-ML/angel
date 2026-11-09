package com.tencent.angel.spark.ml.util

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, Matrix, SliceVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.{DenseMatrix => SDM, DenseVector => SDV, Matrix => SM, SparseMatrix => SSM, SparseVector => SSV, Vector => SV}


object MatrixUtil {
  def matToRowArrs(mat: SM): Array[Array[Double]] = {
    val arrs = new Array[Array[Double]](mat.numRows)
    for (r <- 0 until mat.numRows) {
      arrs(r) = toBreeze(mat)(r to r, 0 until mat.numCols).toDenseMatrix.toArray
    }
    arrs
  }

  def toBreeze(sparkMatrix: SM): Matrix[Double] = {
    sparkMatrix match {
      case dm: SDM =>
        if (!dm.isTransposed) {
          new DenseMatrix[Double](dm.numRows, dm.numCols, dm.values)
        } else {
          val breezeMatrix = new DenseMatrix[Double](dm.numCols, dm.numRows, dm.values)
          breezeMatrix.t
        }
      case sm: SSM =>
        if (!sm.isTransposed) {
          new CSCMatrix[Double](sm.values, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices)
        } else {
          val breezeMatrix =
            new CSCMatrix[Double](sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
          breezeMatrix.t
        }
      case _ =>
        throw new Exception(
          s"Do not support conversion from type ${sparkMatrix.getClass.getName}.")
    }
  }

  def toBreeze(sparkVector: SV): Vector[Double] = {
    sparkVector match {
      case v: SDV =>
        new DenseVector[Double](v.values)
      case v: SSV =>
        new SparseVector[Double](v.indices, v.values, v.size)
    }
  }


  def fromBreeze(breeze: Matrix[Double]): SM = {
    breeze match {
      case dm: DenseMatrix[Double] =>
        new SDM(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: CSCMatrix[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
        new SSM(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new Exception(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }

  def fromBreeze(breezeVector: Vector[Double]): SV = {
    breezeVector match {
      case v: DenseVector[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new SDV(v.data)
        } else {
          new SDV(v.toArray)  // Can't use underlying array directly, so make a new one
        }
      case v: SparseVector[Double] =>
        if (v.index.length == v.used) {
          new SSV(v.length, v.index, v.data)
        } else {
          new SSV(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: SliceVector[_, Double] =>
        new SDV(v.toArray)
      case v: Vector[_] =>
        throw new Exception("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

}

