package com.tencent.angel.ml.model

import java.util.concurrent.Future
import java.util.{ArrayList, List}

import com.tencent.angel.conf.MatrixConfiguration
import com.tencent.angel.exception.{AngelException, InvalidParameterException}
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.updater.base.{UpdaterFunc, VoidResult, ZeroUpdater}
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.psagent.matrix.transport.adapter.{GetRowsResult, RowIndex}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.Map

/**
  * Angel's Core Abstraction. PSModel is used on workers to manipulate distribute models on PSServer.
  *
  * @param modelName
  * @param row
  * @param col
  * @param blockRow
  * @param blockCol
  * @param ctx
  * @tparam K
  */
class PSModel[K <: TVector](val modelName: String, row: Int, col: Int, blockRow: Int = -1, blockCol: Int = -1)(implicit ctx:TaskContext)  {

  val LOG: Log = LogFactory.getLog(classOf[PSModel[K]])

  val matrixCtx =  new MatrixContext(modelName, row, col, blockRow, blockCol)

  def getTaskContext = ctx
  def getContext = matrixCtx
  def getClient = ctx.getMatrix(modelName)

  @throws(classOf[AngelException])
  def clock(): Future[VoidResult] = {
    try {
      return getClient.clock
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  @throws(classOf[AngelException])
  def clock(flush: Boolean): Future[VoidResult] = {
    try {
      return getClient.clock(flush)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  override def finalize(): Unit = super.finalize()

  @throws(classOf[AngelException])
  def flush(): Future[VoidResult] = {
    try {
      return getClient.flush
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  @throws(classOf[AngelException])
  def increment(delta: TVector) {
    try {
      getClient.increment(delta)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  @throws(classOf[AngelException])
  def increment(rowIndex: Int, delta: TVector) {
    try {
      getClient.increment(rowIndex, delta)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  @throws(classOf[AngelException])
  def increment(deltas: List[TVector]) {
    import scala.collection.JavaConversions._
    for (delta <- deltas) increment(delta)
  }

  @throws(classOf[AngelException])
  def get(func: GetFunc): GetResult = {
    try {
      return getClient.get(func)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }



  def getMatrixId(): Int = {
    try {
      return getClient.getMatrixId
    }
    catch {
      case e: InvalidParameterException => {
        return -1
      }
    }
  }


  @SuppressWarnings(Array("unchecked"))
  @throws(classOf[AngelException])
  def getRow(rowId: Int): K = {
    try {
      return getClient.getRow(rowId).asInstanceOf[K]
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }


  @throws(classOf[AngelException])
  def getRows(rowIndex: RowIndex, batchNum: Int): Map[Int, K] = {
    val indexToVectorMap = scala.collection.mutable.Map[Int, K]()
    val rows  = getRowsFlow(rowIndex, batchNum)
    try {
      var finish = false
      while (!finish) {
        rows.take() match {
          case null => finish = true
          case row => indexToVectorMap += (row.getRowId -> row.asInstanceOf[K])
        }
      }
    }
    catch {
      case e: Exception => {
        throw new AngelException(e)
      }
    }
    indexToVectorMap
  }

  @throws(classOf[AngelException])
  def getRows(rowIndexes:Array[Int]): List[K] = {
    val rowIndex = new RowIndex()
    for(index <- rowIndexes) {
      rowIndex.addRowId(index)
    }

    val indexToVectorMap = getRows(rowIndex, -1)

    val rowList = new ArrayList[K](rowIndexes.length)

    for (i <- 0 until rowIndexes.length)
      rowList.add(indexToVectorMap.get(rowIndexes(i)).get)

    rowList
  }

  @throws(classOf[AngelException])
  def getRowsFlow(rowIndex: RowIndex, batchNum: Int): GetRowsResult = {
    try {
      return getClient.getRowsFlow(rowIndex, batchNum)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  def setAttribute(key: String, value: String) {
    matrixCtx.set(key, value)
  }

  def setAverage(aver: Boolean) {
    matrixCtx.set(MatrixConfiguration.MATRIX_AVERAGE, String.valueOf(aver))
  }

  def setHogwild(hogwild: Boolean) {
    matrixCtx.set(MatrixConfiguration.MATRIX_HOGWILD, String.valueOf(hogwild))
  }

  def setOplogType(oplogType: String) {
    matrixCtx.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, oplogType)
  }

  def setRowType(rowType: MLProtos.RowType) {
    matrixCtx.setRowType(rowType)
  }

  def setLoadPath(path: String) {
    matrixCtx.set(MatrixConfiguration.MATRIX_LOAD_PATH, path)
    LOG.info("Before training, matrix " + this.matrixCtx.getName + " will be loaded from " + path)
  }

  def setSavePath(path: String) {
    matrixCtx.set(MatrixConfiguration.MATRIX_SAVE_PATH, path)
    LOG.info("After training matrix " + this.matrixCtx.getName + " will be saved to " + path)
  }

  @throws(classOf[AngelException])
  def update(func: UpdaterFunc): Future[VoidResult] = {
    try {
      return getClient.update(func)
    }
    catch {
      case e: InvalidParameterException => {
        throw new AngelException(e)
      }
    }
  }

  @throws(classOf[AngelException])
  def zero() {
    val updater: ZeroUpdater = new ZeroUpdater(new ZeroUpdater.ZeroUpdaterParam(getMatrixId, false))
    try {
      update(updater).get
    }
    catch {
      case e: Any => {
        throw new AngelException(e)
      }
    }
  }

}

object PSModel {
  def apply[K <: TVector](modelName: String, row: Int, col: Int, blockRow: Int = -1, blockCol: Int = -1)(implicit ctx:TaskContext) = {
    new PSModel[K](modelName, row, col, blockRow, blockCol)(ctx)
  }
}

