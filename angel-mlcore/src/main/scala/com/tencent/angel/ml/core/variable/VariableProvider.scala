package com.tencent.angel.ml.core.variable

import com.tencent.angel.ml.core.network.Graph

abstract class VariableProvider()(implicit graph: Graph) {
  def getMatVariable(name:String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable

  def getEmbedVariable(name:String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String): EmbedVariable

  def getVecVariable(name:String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable
}
