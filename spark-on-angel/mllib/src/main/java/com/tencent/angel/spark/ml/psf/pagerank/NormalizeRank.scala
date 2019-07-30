package com.tencent.angel.spark.ml.psf.pagerank

import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow, ServerRowUtils}

/**
  * PSFunction for normalizing page ranks in PageRank algorithm
  */

class NormalizeRank(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, rankSum: Double, numNodes: Double) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](rankSum, numNodes)))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val ranks = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongFloatRow])
    val rankSum = scalars(0).toFloat
    val numNodes = scalars(1).toFloat
    ranks.imul(numNodes / rankSum)
//    ranks match {
//      case r: IntFloatVector =>
//        val it = r.getStorage.entryIterator()
//        while (it.hasNext) {
//          val entry = it.next()
//          r.set(entry.getIntKey, entry.getFloatValue * numNodes / rankSum)
//        }
//
//      case r: LongFloatVector =>
//        val it = r.getStorage.entryIterator()
//        while (it.hasNext) {
//          val entry = it.next()
//          r.set(entry.getLongKey, entry.getFloatValue * numNodes / rankSum)
//        }
//    }
  }
}
