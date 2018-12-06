package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.Graph

abstract class VariableProvider()(implicit graph: Graph) {
  def getMatVariable(name:String, numRows: Int, numCols: Long, numSlot: Int, inIPLayer: Boolean): MatVariable

  def getEmbedVariable(name:String, numRows: Int, numCols: Long, numSlot: Int): EmbedVariable

  def getVecVariable(name:String, length: Long, numSlot: Int, inIPLayer: Boolean): VecVariable
}
