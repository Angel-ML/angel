package com.tencent.angel.ml.core.variable

abstract class VariableProvider(implicit val variableManager: VariableManager) {
  def getMatVariable(name:String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable

  def getEmbedVariable(name:String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, taskNum: Int): EmbedVariable

  def getVecVariable(name:String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable
}
