package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{MatrixLoadContext, ModelLoadContext, ModelSaveContext, ModelTools}
import org.apache.hadoop.conf.Configuration

abstract class LocalVariable(name: String, rowType: RowType)(implicit graph: Graph)
  extends Variable(name, rowType, Location.Local)(graph) {
  protected val rowsSaved: Array[Int]
  protected var storage: Matrix = _

  def create(): Unit

  def loadParams(loadContext: ModelLoadContext): Unit = {
    storage = ModelTools.loadToLocal(new MatrixLoadContext(name, loadContext.getLoadPath), new Configuration)
  }

  def saveParams(saveContext: ModelSaveContext): Unit = ???
}
