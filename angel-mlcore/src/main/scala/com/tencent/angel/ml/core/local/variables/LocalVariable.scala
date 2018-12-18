package com.tencent.angel.ml.core.local.variables

import java.io.File

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, ModelTools}
import org.apache.hadoop.conf.Configuration


abstract class LocalVariable(name: String, rowType: RowType)(implicit graph: Graph)
  extends Variable(name, rowType) {
  var storage: Matrix = _

  def create(): Unit

  override def load(): Unit = {
    val loadPath = SharedConf.get().getString(MLConf.ML_LOAD_MODEL_PATH)
    val pathName = s"$loadPath${File.separator}$name"
    storage = ModelTools.loadToLocal(new MatrixLoadContext(name, pathName), new Configuration())
  }

  override def save(): Unit = ???
}
