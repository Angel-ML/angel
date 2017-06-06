package com.tencent.angel.ml.model

import java.util.{ArrayList, List}
import java.util.concurrent.Future

import com.tencent.angel.conf.MatrixConfiguration
import com.tencent.angel.exception.{AngelException, InvalidParameterException}
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.updater.base.{UpdaterFunc, VoidResult, ZeroUpdater}
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.psagent.matrix.MatrixClient
import com.tencent.angel.psagent.matrix.transport.adapter.{GetRowsResult, RowIndex}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import scala.collection.mutable.Map

class PSModel[K <: TVector] {
  val LOG: Log = LogFactory.getLog(classOf[PSModel[K]])

  private var martrixCtx: MatrixContext = null
  private var ctx: TaskContext = null
  private var client: MatrixClient = null


  def this(name: String, row: Int, col: Int, blockRow: Int, blockCol: Int) {
    this()
    martrixCtx = new MatrixContext(name, row, col, blockRow, blockCol)
  }

  def this(ctx: TaskContext, name: String, row: Int, col: Int, blockRow: Int, blockCol: Int) {
    this(name, row, col, blockRow, blockCol)
    this.ctx = ctx
  }

  override def finalize(): Unit = super.finalize()

  def this(name: String, row: Int, col: Int) {
    this()
    martrixCtx = new MatrixContext(name, row, col)
  }

  def this(ctx: TaskContext, name: String, row: Int, col: Int) {
    this(name, row, col)
    this.ctx = ctx
  }

  def this(matrixCtx: MatrixContext) {
    this()
    this.martrixCtx = matrixCtx
  }


  private def getClient: MatrixClient = {
    if (client == null) client = ctx.getMatrix(getName)
    return client
  }

  def getContext(): MatrixContext = {
    return martrixCtx
  }

  def getName(): String = {
    return martrixCtx.getName
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

  def setAttribute(key: String, value: String) {
    martrixCtx.set(key, value)
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
  def increment(rowId: Int, delta: TVector) {
    try {
      getClient.increment(rowId, delta)
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


  @SuppressWarnings(Array("unchecked"))
  @throws(classOf[AngelException])
  def getRows(rowIndex: RowIndex, batchNum: Int): Map[Int, K] = {
    val indexToVectorMap = scala.collection.mutable.Map[Int, K]()
    val rows  = getRowsFlow(rowIndex, batchNum)
    try {
      while (true) {
        val row = rows.take
        if (row != null) indexToVectorMap += (row.getRowId -> row.asInstanceOf[K])
      }
    }
    catch {
      case e: Exception => {
        throw new AngelException(e)
      }
    }
    return indexToVectorMap
  }
  
  @SuppressWarnings(Array("unchecked"))
  @throws(classOf[AngelException])
  def getRows(rowIndexes:Array[Int]): List[K] = {
    val rowIndex = new RowIndex()
    for(index <- rowIndexes) {
      rowIndex.addRowId(index)
    }
       
    val indexToVectorMap = scala.collection.mutable.Map[Int, K]()
    val rows  = getRowsFlow(rowIndex, -1)
    try {
      while (true) {
        val row = rows.take
        if (row != null) indexToVectorMap += (row.getRowId -> row.asInstanceOf[K])
      }
    }
    catch {
      case e: Exception => {
        throw new AngelException(e)
      }
    }
    
    val rowList = new ArrayList[K](rowIndexes.length)
    var i = 0
    for(index <- rowIndexes) {
      rowList.set(i, indexToVectorMap.get(index).get)
      i = i + 1
    }
    return rowList
  }

  def setAverage(aver: Boolean) {
    martrixCtx.set(MatrixConfiguration.MATRIX_AVERAGE, String.valueOf(aver))
  }

  def setHogwild(hogwild: Boolean) {
    martrixCtx.set(MatrixConfiguration.MATRIX_HOGWILD, String.valueOf(hogwild))
  }

  def setOplogType(oplogType: String) {
    martrixCtx.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, oplogType)
  }

  def setRowType(rowType: MLProtos.RowType) {
    martrixCtx.setRowType(rowType)
  }

  def setLoadPath(path: String) {
    martrixCtx.set(MatrixConfiguration.MATRIX_LOAD_PATH, path)
    LOG.info("Before training, matrix " + this.martrixCtx.getName + " will be loaded from " + path)
  }

  def setSavePath(path: String) {
    martrixCtx.set(MatrixConfiguration.MATRIX_SAVE_PATH, path)
    LOG.info("After training matrix " + this.martrixCtx.getName + " will be saved to " + path)
  }
}
