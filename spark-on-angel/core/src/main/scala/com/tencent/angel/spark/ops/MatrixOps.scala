package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.aggr.FullPull
import com.tencent.angel.ml.matrix.psf.aggr.enhance.FullAggrResult
import com.tencent.angel.ml.matrix.psf.common.{Fill, Increment}
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.{Diag, Eye, FullFill}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.matrix.PSMatrix

class MatrixOps {

  /**
   * Pull matrix to local
   */
  def pull(mat: PSMatrix): Array[Array[Double]] = {
    mat.assertValid()
    aggregate(mat, new FullPull(mat.meta.getId)).asInstanceOf[FullAggrResult].getResult
  }


  /**
   * Push local data to update matrix in PS
   */
  def push(mat: PSMatrix, pairs: Array[(Int, Long, Double)]): Unit = {
    mat.assertValid()

    val rows = new Array[Int](pairs.length)
    val cols = new Array[Long](pairs.length)
    val values = new Array[Double](pairs.length)
    pairs.indices.foreach { i =>
      rows(i) = pairs(i)._1
      cols(i) = pairs(i)._2
      values(i) = pairs(i)._3
    }
    update(mat, new Fill(mat.meta.getId, rows, cols, values))
  }

  def pull(mat: PSMatrix, pairs: Array[(Int, Long)]): Unit = {

  }

  def increment(mat: PSMatrix, pairs: Array[(Int, Long, Double)]): Unit = {
    mat.assertValid()

    val rows = new Array[Int](pairs.length)
    val cols = new Array[Long](pairs.length)
    val values = new Array[Double](pairs.length)
    pairs.indices.foreach { i =>
      rows(i) = pairs(i)._1
      cols(i) = pairs(i)._2
      values(i) = pairs(i)._3
    }
    update(mat, new Increment(mat.meta.getId, rows, cols, values))
  }


  /**
    * Assign matrix diagonal with `value`
    */
  def diag(mat: PSMatrix, value: Array[Double]): Unit = {
    mat.assertValid()
    mat.assertCompatible(value)
    assert(mat.meta.getColNum == mat.meta.getRowNum, s"when init diag matrix, " +
      s"matrix columnNum(${mat.meta.getColNum}) must equal to rowNum(${mat.meta.getRowNum})")

    update(mat, new Diag(mat.meta.getId, value))
  }

  /**
    * Assign matrix diagonal with 1.0
    */
  def eye(mat: PSMatrix): Unit = {
    mat.assertValid()
    assert(mat.meta.getColNum == mat.meta.getRowNum, s"when init eye matrix, " +
      s"matrix columnNum(${mat.meta.getColNum}) must equal to rowNum(${mat.meta.getRowNum})")
    update(mat, new Eye(mat.meta.getId))

  }

  /**
    * Fill matrix with `value`
    */
  def fill(mat: PSMatrix, value: Double): Unit = {
    mat.assertValid()
    update(mat, new FullFill(mat.meta.getId, value))
  }



  /**
   * the following are private methods
   */
  private[spark] def aggregate(matrix: PSMatrix, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(matrix.meta.getId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  private def update(matrix: PSMatrix, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(matrix.meta.getId, PSContext.getTaskId())
    val result = client.update(func).get()
    assertSuccess(result)
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }


}
