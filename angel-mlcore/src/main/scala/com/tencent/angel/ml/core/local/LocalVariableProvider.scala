package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalEmbedVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}

class LocalVariableProvider(implicit graph: Graph) extends VariableProvider {
  override def getEmbedVariable(name: String, numRows: Int, numCols: Long, updater: Updater): EmbedVariable = {
    new LocalEmbedVariable(name, numRows, numCols, updater, graph.modelType, true)
  }

  override def getMatVariable(name: String, numRows: Int, numCols: Long, updater: Updater, withInput: Boolean): MatVariable = {
    (graph.dataFormat, withInput) match {
      case ("dense", true) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater, graph.modelType, withInput)
      case ("libsvm"| "dummy", true) =>
        new LocalMatVariable(name, numRows, numCols, updater, graph.modelType, withInput)
      case (_, false) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater,
          RowTypeUtils.getDenseModelType(graph.modelType), withInput)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, withInput: Boolean): VecVariable = {
    if (withInput) {
      new LocalVecVariable(name, length, updater, graph.modelType, withInput)
    } else {
      new LocalVecVariable(name, length, updater, RowTypeUtils.getDenseModelType(graph.modelType), withInput)
    }
  }
}
