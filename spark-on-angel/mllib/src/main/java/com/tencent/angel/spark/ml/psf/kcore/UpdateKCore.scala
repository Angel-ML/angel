package com.tencent.angel.spark.ml.psf.kcore

import com.tencent.angel.ml.math2.vector.{IntIntVector, LongIntVector, Vector}
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongIntRow, ServerRow, ServerRowUtils}

class UpdateKCore(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowIds: Array[Int]) =
    this(new MMUpdateParam(matrixId, rowIds, Array[Double](0)))

  def this() = this(null)

  override protected
  def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val inMsgs = ServerRowUtils.getVector(rows(0).asInstanceOf[ServerLongIntRow])
    val outMsgs = ServerRowUtils.getVector(rows(1).asInstanceOf[ServerLongIntRow])
    val cores = ServerRowUtils.getVector(rows(2).asInstanceOf[ServerLongIntRow])

    (outMsgs, cores) match {
      case (o: IntIntVector, c: IntIntVector) =>
        computeCores(o, c)
      case (o: LongIntVector, c: LongIntVector) =>
        computeCores(o, c)
    }

    switchWriteAndRead(inMsgs, outMsgs)
  }

  def computeCores(writeMsgs: IntIntVector, cores: IntIntVector): Unit = {
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getIntKey
      val msg = entry.getIntValue
      cores.set(key, msg)
    }
  }

  def computeCores(writeMsgs: LongIntVector, cores: LongIntVector): Unit = {
    val it = writeMsgs.getStorage.entryIterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getLongKey
      val msg = entry.getIntValue
      cores.set(key, msg)
    }
  }

  def switchWriteAndRead(inMsgs: Vector, outMsgs: Vector): Unit = {
    val storage = inMsgs.getStorage
    storage.clear()
    inMsgs.setStorage(outMsgs.getStorage)
    outMsgs.setStorage(storage)
  }

}
