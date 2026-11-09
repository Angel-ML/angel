package com.tencent.angel.spark.ml.util

import org.apache.spark.ml.linalg.{DenseMatrix, Matrix, Vector}


object LagUtil {
  /**
    * Makes a lag matrix from the given time series with the given lag, trimming both rows and
    * columns so that every element in the matrix is full.
    */
  def lagMatTrimBoth(x: Array[Double], maxLag: Int): Array[Array[Double]] = {
    lagMatTrimBoth(x, maxLag, includeOriginal = false)
  }

  /**
    * Makes a lag matrix from the given time series with the given lag, trimming both rows and
    * columns so that every element in the matrix is full.
    */
  def lagMatTrimBoth(x: Array[Double], maxLag: Int, includeOriginal: Boolean)
  : Array[Array[Double]] = {
    val numObservations = x.length
    val numRows = numObservations - maxLag
    val numCols = maxLag + (if (includeOriginal) 1 else 0)
    val lagMat = Array.ofDim[Double](numRows, numCols)

    val initialLag = if (includeOriginal) 0 else 1

    for (r <- 0 until numRows) {
      for (c <- initialLag to maxLag) {
        lagMat(r)(c - initialLag) = x(r + maxLag - c)
      }
    }
    lagMat
  }

  /**
    * Makes a lag matrix from the given time series with the given lag, trimming both rows and
    * columns so that every element in the matrix is full.
    */
  def lagMatTrimBoth(x: Vector, maxLag: Int): Matrix = {
    lagMatTrimBoth(x, maxLag, includeOriginal = false)
  }

  /**
    * Makes a lag matrix from the given time series with the given lag, trimming both rows and
    * columns so that every element in the matrix is full.
    */
  def lagMatTrimBoth(x: Vector, maxLag: Int, includeOriginal: Boolean): Matrix = {
    val numObservations = x.size
    val numRows = numObservations - maxLag
    val numCols = maxLag + (if (includeOriginal) 1 else 0)
    val lagMat = new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))

    lagMatTrimBoth(x, lagMat, maxLag, includeOriginal, 0)
    lagMat
  }

  /**
    * @param x Vector to be lagged.
    * @param outputMat Matrix to place the lagged vector into, as a column.
    * @param numLags The number of times to lag the vector.  E.g. if this is 2, the output matrix
    *                will include one column that is the vector lagged by 1, and another column to
    *                the right that is the vector lagged by 2.
    * @param includeOriginal Whether to place the original time series into the matrix as well.
    * @param colOffset The offset to start placing columns in the output mat.
    */
  def lagMatTrimBoth(x: Vector,
                     outputMat: DenseMatrix,
                     numLags: Int,
                     includeOriginal: Boolean,
                     colOffset: Int): Unit = {
    val numRows = outputMat.numRows
    val numTruncatedRows = x.size - numRows

    val initialLag = if (includeOriginal) 0 else 1

    val breezeOutputMat = MatrixUtil.toBreeze(outputMat)
    for (r <- 0 until numRows) {
      for (lag <- initialLag to numLags) {
        val c = colOffset + lag - initialLag
        breezeOutputMat(r, c) = x(r + numTruncatedRows - lag)
      }
    }
  }

  /**
    * Creates a lagged matrix from a current matrix (represented in row-array form).
    * Lags each column the appropriate amount of times and then concatenates the columns.
    * So given a matrix [a b c], where a/b/c are column vectors, and calling with lag of 2,
    * becomes a matrix of the form [a_-1 a_-2 b_-1 b_-2 c_-1 c_-2]
    */
  def lagMatTrimBoth(x: Array[Array[Double]],
                     maxLag: Int,
                     includeOriginal: Boolean): Array[Array[Double]] = {
    val xt = x.transpose
    // one matrix per column, consisting of all its lags
    val matrices = for (col <- xt) yield {
      LagUtil.lagMatTrimBoth(col, maxLag, includeOriginal)
    }
    // merge the matrices into 1 matrix by concatenating col-wise
    matrices.transpose.map(_.reduceLeft(_ ++ _))
  }

  /**
    * Creates a lagged matrix from a current matrix (represented in row-array form).
    * Lags each column the appropriate amount of times and then concatenates the columns.
    * So given a matrix [a b c], where a/b/c are column vectors, and calling with lag of 2,
    * becomes a matrix of the form [a_-1 a_-2 b_-1 b_-2 c_-1 c_-2]
    * The original time series is not included in the matrix.
    */
  def lagMatTrimBoth(x: Array[Array[Double]], maxLag: Int): Array[Array[Double]] = {
    lagMatTrimBoth(x, maxLag, includeOriginal = false)
  }
}

