package com.tencent.angel.spark.ml.psf.pagerank

import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.FloatVector
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class ComputeRank2(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int], initRank: Float, resetProb: Float) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](initRank, resetProb)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val outMsgs = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongFloatRow])
    val ranks = ServerRowUtils.getVector(rows(2).asInstanceOf[ServerLongFloatRow])
    Ufuncs.ipagerank(ranks, outMsgs, scalars(0).toFloat, scalars(1).toFloat)
    switchWriteAndRead(inMsgs, outMsgs)
  }

  def switchWriteAndRead(inMsgs: FloatVector, outMsgs: FloatVector): Unit = {
    val tmp = inMsgs.getStorage
    inMsgs.setStorage(outMsgs.getStorage)
    outMsgs.setStorage(tmp)
    tmp.clear()
  }

}
