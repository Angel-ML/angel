package com.tencent.angel.ml.core.local.variables

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.utils.RowTypeUtils

class LocalVariableProvider(implicit graph: Graph) extends VariableProvider {
  override def getEmbedVariable(name: String, numRows: Int, numCols: Long, numSlot: Int): EmbedVariable = {
    new LocalEmbedVariable(name, numRows, numCols, numSlot, graph.modelType)
  }

  override def getMatVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, inIPLayer: Boolean): MatVariable = {
    (graph.dataFormat, inIPLayer) match {
      case (_, false) =>
        new LocalBlasMatVariable(name, numRows, numCols, numSlot, RowTypeUtils.getDenseModelType(graph.modelType))
      case ("dense", true) =>
        new LocalBlasMatVariable(name, numRows, numCols, numSlot, graph.modelType)
      case ("libsvm"| "dummy", true) =>
        new LocalMatVariable(name, numRows, numCols, numSlot, graph.modelType)
    }
  }

  override def getVecVariable(name: String, length: Long, numSlot: Int, inIPLayer: Boolean): VecVariable = {
    if (inIPLayer) {
      new LocalVecVariable(name, length, numSlot, graph.modelType)
    } else {
      new LocalVecVariable(name, length, numSlot, RowTypeUtils.getDenseModelType(graph.modelType))
    }
  }
}
