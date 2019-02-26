package com.tencent.angel.ml.core.variable

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.core.AngelEvnContext
import com.tencent.angel.ml.core.conf.{AngelMLConf, SharedConf}
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}

abstract class PSVariable(name: String, rowType: RowType, updater: Updater, formatClassName: String,
                          allowPullWithIndex: Boolean)(implicit graph: Graph)
  extends Variable(name, rowType, updater, formatClassName, allowPullWithIndex) {

  protected var matrixId: Int = -1
  protected var ctx: MatrixContext = _

  def getMatrixId: Int = matrixId

  val numFactors: Int

  def getMatrixCtx: MatrixContext

  protected def rowsSaved(withSlot: Boolean = false): Array[Int]

  protected def doCreate(envCtx: EvnContext): Unit = {
    val angelEvnContext: AngelEvnContext = envCtx.asInstanceOf[AngelEvnContext]

    if (angelEvnContext.angelClient != null) {
      // create matrix in angel client (on client)
      val mcList = new util.ArrayList[MatrixContext]()
      mcList.add(getMatrixCtx)
      angelEvnContext.angelClient.createMatrices(mcList)
      // the matrix is created, but the is no PSAgent in client, so cannot call:
      // matrixId = PSMatrixUtils.getMatrixId(name)
    } else {
      // create matrix in work, on worker
      // in fact, the matrix has created, just get the matrixId here
      matrixId = PSMatrixUtils.getMatrixId(name)
    }
  }

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    writeLock.lock()

    try {
      this.mean = mean
      this.stddev = stddev

      if (state == VarState.Created) {
        val loadModelPath = SharedConf.get().get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")
        if (taskFlag == 0 && rowType.isDense && loadModelPath.isEmpty) {
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          doInit(taskFlag)
        }

        // trans stats
        transSate(VarState.Created, VarState.Initialized)
      }

      assert(state == VarState.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  // call only on client
  protected override def doLoad(envCtx: EvnContext, path: String): Unit = {
    val angelEvnContext = envCtx.asInstanceOf[AngelEvnContext]
    if (angelEvnContext.angelClient != null) {
      val angelClient = angelEvnContext.angelClient
      val loadContext = new ModelLoadContext(path)
      loadContext.addMatrix(new MatrixLoadContext(name))
      angelClient.load(loadContext)
    } else {
      if (matrixId == -1) {
        matrixId = PSMatrixUtils.getMatrixId(name)
      }

      if (ctx == null) {
        ctx = getMatrixCtx
      }
    }
  }

  protected override def doSave(envCtx: EvnContext, path: String): Unit = {
    val angelEvnContext: AngelEvnContext = envCtx.asInstanceOf[AngelEvnContext]
    val withSlot: Boolean = conf.getBoolean(AngelMLConf.ML_VERABLE_SAVE_WITHSLOT,
      AngelMLConf.DEFAULT_ML_VERABLE_SAVE_WITHSLOT)

    if (angelEvnContext.angelClient != null) {
      val angelClient = angelEvnContext.angelClient
      val saveContext: ModelSaveContext = new ModelSaveContext(path)
      val msc: MatrixSaveContext = new MatrixSaveContext(name, formatClassName)
      msc.addIndices(rowsSaved(withSlot))
      saveContext.addMatrix(msc)

      if (PSVariable.isFirstSave.getAndSet(false)) {
        val deleteExistsFile = conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
          AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
        angelClient.save(saveContext, deleteExistsFile)
      } else {
        angelClient.save(saveContext, false)
      }
    }
  }
}

object PSVariable {
  var isFirstSave: AtomicBoolean = new AtomicBoolean(true)
}
