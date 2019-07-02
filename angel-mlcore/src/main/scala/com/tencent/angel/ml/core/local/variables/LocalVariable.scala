package com.tencent.angel.ml.core.local.variables

import java.io.File

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.variable.{Updater, VarState, Variable, VariableManager}
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, ModelTools}
import org.apache.hadoop.conf.Configuration


abstract class LocalVariable(name: String,
                             rowType: RowType,
                             updater: Updater,
                             formatClassName: String,
                             allowPullWithIndex: Boolean)
                            (implicit conf: SharedConf, variableManager: VariableManager)
  extends Variable(name, rowType, updater, formatClassName, allowPullWithIndex) {
  var storage: Matrix = _

  protected override def doLoad[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    // val loadPath = SharedConf.get().getString(MLConf.ML_LOAD_MODEL_PATH)
    assert(envCtx == null || envCtx.client == null)
    val pathName = s"$path${File.separator}$name"
    storage = ModelTools.loadToLocal(new MatrixLoadContext(name, pathName), conf)
  }

  protected override def doSave[T](envCtx: EnvContext[T], path: String): Unit = {
    assert(envCtx == null || envCtx.client == null)
    assert(state == VarState.Ready || state == VarState.Initialized)
  }
}
