package com.tencent.angel.spark.ml.psf.pagerank

import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class DivSums(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int], resetProb: Float, tol: Float) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](resetProb, tol)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val sums = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongFloatRow])
    inMsgs.imul(1 - scalars(0))
    UnaryExecutor.apply(inMsgs, new LessZero(true, scalars(1).toFloat))
    inMsgs.idiv(sums)
  }

}
