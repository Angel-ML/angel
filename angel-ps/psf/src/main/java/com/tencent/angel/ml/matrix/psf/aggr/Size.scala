package com.tencent.angel.ml.matrix.psf.aggr

import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow

class Size(matrixId: Int, rowId: Int) extends UnaryAggrFunc(matrixId, rowId) {

  def this() = this(-1, -1)

  override protected def processRow(row: ServerRow): Double = {
    row.size()
  }

  override protected def mergeInit: Double = 0.0

  override protected def mergeOp(a: Double, b: Double): Double = a + b

}
