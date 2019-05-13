package com.tencent.angel.ml.core.variable

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.core.AngelEnvContext
import com.tencent.angel.ml.core.conf.{AngelMLConf, SharedConf}
import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model._
import org.apache.hadoop.conf.Configuration


abstract class PSVariable(name: String,
                          rowType: RowType,
                          updater: Updater,
                          formatClassName: String,
                          allowPullWithIndex: Boolean)
                         (implicit variableManager: VariableManager, cilsImpl: CILSImpl)
  extends Variable(name, rowType, updater, formatClassName, allowPullWithIndex) {

  protected var matrixId: Int = -1
  protected var ctx: MatrixContext = _

  def getMatrixId: Int = matrixId

  val numFactors: Int

  def getMatrixCtx: MatrixContext

  protected def rowsSaved(withSlot: Boolean = false): Array[Int]

  protected def doCreate[T](envCtx: EnvContext[T]): Unit = {
    if (envCtx != null && envCtx.client != null) {
      cilsImpl.doCreate(getMatrixCtx, envCtx)
    } else {
      // create matrix in work, on worker
      // in fact, the matrix has created, just get the matrixId here
      if (matrixId == -1) {
        matrixId = PSMatrixUtils.getMatrixId(name)
      }

      if (ctx == null) {
        ctx = getMatrixCtx
      }
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
  protected override def doLoad[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    if (envCtx != null && envCtx.client != null) {
      cilsImpl.doLoad(getMatrixCtx, envCtx, path)
    } else {
      if (matrixId == -1) {
        matrixId = PSMatrixUtils.getMatrixId(name)
      }

      if (ctx == null) {
        ctx = getMatrixCtx
      }
    }
  }

  protected override def doSave[T](envCtx: EnvContext[T], path: String): Unit = {
    if (envCtx != null && envCtx.client != null) {
      val withSlot: Boolean = conf.getBoolean(AngelMLConf.ML_VERABLE_SAVE_WITHSLOT,
        AngelMLConf.DEFAULT_ML_VERABLE_SAVE_WITHSLOT)
      val indices = rowsSaved(withSlot)
      cilsImpl.doSave(getMatrixCtx, indices, envCtx, path)
    }
  }
}

object PSVariable {
  var isFirstSave: AtomicBoolean = new AtomicBoolean(true)
}
