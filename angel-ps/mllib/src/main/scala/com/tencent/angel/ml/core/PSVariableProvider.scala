package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}
import com.tencent.angel.ml.core.variable._

class PSVariableProvider(implicit graph: Graph) extends VariableProvider {
  private val validIndexNum: Long = SharedConf.modelSize

  override def getEmbedVariable(name: String, numRows: Int, numCols: Long, updater: Updater, formatClassName: String): EmbedVariable = {
    new PSEmbedVariable(name, numRows, numCols, validIndexNum, updater,
      RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, true)
  }

  override def getMatVariable(name: String, numRows: Int, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable = {
    (graph.dataFormat, allowPullWithIndex) match {
      case ("dense", true) =>
        new PSBlasMatVariable(name, numRows, numCols, updater, graph.modelType, formatClassName, allowPullWithIndex)
      case ("libsvm" | "dummy", true) =>
        new PSMatVariable(name, numRows, numCols, validIndexNum, updater, graph.modelType, formatClassName, allowPullWithIndex)
      case (_, false) =>
        new PSBlasMatVariable(name, numRows, numCols, updater,
          RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, allowPullWithIndex)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable = {
    if (allowPullWithIndex) {
      new PSVecVariable(name, length, validIndexNum, updater, graph.modelType, formatClassName, true)
    } else {
      new PSVecVariable(name, length, length, updater, RowTypeUtils.getDenseModelType(graph.modelType), formatClassName, false)
    }
  }
}
