package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalEmbedVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}

class LocalVariableProvider(implicit graph: Graph) extends VariableProvider {
  override def getEmbedVariable(name: String, numRows: Int, numCols: Long, updater: Updater): EmbedVariable = {
    new LocalEmbedVariable(name, numRows, numCols, updater,
      RowTypeUtils.getDenseModelType(graph.modelType), true)
  }

  override def getMatVariable(name: String, numRows: Int, numCols: Long, updater: Updater, allowPullWithIndex: Boolean): MatVariable = {
    (graph.dataFormat, allowPullWithIndex) match {
      case ("dense", true) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater, graph.modelType, allowPullWithIndex)
      case ("libsvm"| "dummy", true) =>
        new LocalMatVariable(name, numRows, numCols, updater, graph.modelType, allowPullWithIndex)
      case (_, false) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater,
          RowTypeUtils.getDenseModelType(graph.modelType), allowPullWithIndex)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, allowPullWithIndex: Boolean): VecVariable = {
    if (allowPullWithIndex) {
      new LocalVecVariable(name, length, updater, graph.modelType, allowPullWithIndex)
    } else {
      new LocalVecVariable(name, length, updater, RowTypeUtils.getDenseModelType(graph.modelType), allowPullWithIndex)
    }
  }
}
