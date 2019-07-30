package com.tencent.angel.spark.ml.psf.pagerank

import com.tencent.angel.ml.math2.vector.{IntFloatVector, LongFloatVector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

class InsertLeakedRank (param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, incVal: Double) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](incVal)))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val ranks = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val leakedRank = scalars(0).toFloat
    ranks match {
      case r: IntFloatVector =>
        val it = r.getStorage.entryIterator()
        while (it.hasNext) {
          val entry = it.next()
          r.set(entry.getIntKey, entry.getFloatValue + leakedRank)
        }

      case r: LongFloatVector =>
        val it = r.getStorage.entryIterator()
        while (it.hasNext) {
          val entry = it.next()
          r.set(entry.getLongKey, entry.getFloatValue + leakedRank)
        }
    }
  }

}
