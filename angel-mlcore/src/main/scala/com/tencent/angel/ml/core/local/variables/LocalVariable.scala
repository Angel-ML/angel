package com.tencent.angel.ml.core.local.variables

import java.io.File

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.variable.{Updater, VarState, Variable}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, ModelTools}
import org.apache.hadoop.conf.Configuration


abstract class LocalVariable(name: String, rowType: RowType, updater: Updater, allowPullWithIndex: Boolean)(
  implicit graph: Graph) extends Variable(name, rowType, updater, allowPullWithIndex) {
  var storage: Matrix = _

  override def load(envCtx: EvnContext, path: String): Unit = {
    writeLock.lock()

    try {
      if (state == VarState.New || state == VarState.Created || state == VarState.Expired) {
        // val loadPath = SharedConf.get().getString(MLConf.ML_LOAD_MODEL_PATH)
        val pathName = s"$path${File.separator}$name"
        storage = ModelTools.loadToLocal(new MatrixLoadContext(name, pathName), new Configuration())

        // trans state
        if (state == VarState.New) {
          transSate(VarState.New, VarState.Initialized)
        } else if (state == VarState.Created) {
          transSate(VarState.Created, VarState.Initialized)
        } else {
          transSate(VarState.Expired, VarState.Initialized)
        }
      }

      assert(state == VarState.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  override def save(envCtx: EvnContext, path: String): Unit = this.synchronized {
    assert(state == VarState.Ready || state == VarState.Initialized)
  }
}
