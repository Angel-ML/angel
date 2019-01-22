package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.Graph

abstract class VariableProvider()(implicit graph: Graph) {
  def getMatVariable(name:String, numRows: Int, numCols: Long, updater: Updater, withInput: Boolean): MatVariable

  def getEmbedVariable(name:String, numRows: Int, numCols: Long, updater: Updater): EmbedVariable

  def getVecVariable(name:String, length: Long, updater: Updater, withInput: Boolean): VecVariable
}
