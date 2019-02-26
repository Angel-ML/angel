package com.tencent.angel.ml.core.local.variables

import java.io.File

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.variable.{Updater, VarState, Variable}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, ModelTools}
import org.apache.hadoop.conf.Configuration


abstract class LocalVariable(name: String, rowType: RowType, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean)(
  implicit graph: Graph) extends Variable(name, rowType, updater, formatClassName, allowPullWithIndex) {
  var storage: Matrix = _

  protected override def doLoad(envCtx: EvnContext, path: String): Unit = {
    // val loadPath = SharedConf.get().getString(MLConf.ML_LOAD_MODEL_PATH)
    val pathName = s"$path${File.separator}$name"
    storage = ModelTools.loadToLocal(new MatrixLoadContext(name, pathName), new Configuration())
  }

  protected override def doSave(envCtx: EvnContext, path: String): Unit = {
    assert(state == VarState.Ready || state == VarState.Initialized)
  }
}
