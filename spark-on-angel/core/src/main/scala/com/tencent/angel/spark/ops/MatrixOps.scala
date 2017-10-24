package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.aggr.FullPull
import com.tencent.angel.ml.matrix.psf.aggr.enhance.FullAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.{Diag, Eye, FullFill}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.matrix.PSMatrix

class MatrixOps {


  /**
    * ===================================================
    * The following methods are matrix oriented.
    * ===================================================
    */

  /**
    * Pull matrix to local
    */
  def pull(mat: PSMatrix): Array[Array[Double]] = {
    mat.assertValid()
    aggregate(mat, new FullPull(mat.meta.getId)).asInstanceOf[FullAggrResult].getResult
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

  def aggregate(matrix: PSMatrix, func: GetFunc): GetResult = {
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
