package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalEmbedVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.variable._
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}

class LocalVariableProvider(implicit graph: Graph) extends VariableProvider {
  override def getEmbedVariable(name: String, numRows: Int, numCols: Long, updater: Updater, formatClassName: String): EmbedVariable = {
    new LocalEmbedVariable(name, numRows, numCols, updater,
      RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, true)
  }

  override def getMatVariable(name: String, numRows: Int, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable = {
    (graph.dataFormat, allowPullWithIndex) match {
      case ("dense", true) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater, graph.modelType, formatClassName, allowPullWithIndex)
      case ("libsvm"| "dummy", true) =>
        new LocalMatVariable(name, numRows, numCols, updater, graph.modelType, formatClassName, allowPullWithIndex)
      case (_, false) =>
        new LocalBlasMatVariable(name, numRows, numCols, updater,
          RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, allowPullWithIndex)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable = {
    if (allowPullWithIndex) {
      new LocalVecVariable(name, length, updater, graph.modelType, formatClassName, allowPullWithIndex)
    } else {
      new LocalVecVariable(name, length, updater, RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, allowPullWithIndex)
    }
  }
}
