package com.tencent.angel.ml.core.variable

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.PlaceHolder

abstract class VariableProvider(implicit val conf: SharedConf, val variableManager: VariableManager) {
  def getMatVariable(name: String, numRows: Long, numCols: Long, updater: Updater,
                     formatClassName: String, allowPullWithIndex: Boolean): MatVariable

  def getEmbedVariable(name: String, numRows: Long, numCols: Long, updater: Updater,
                       formatClassName: String, placeHolder: PlaceHolder, taskNum: Int): EmbedVariable

  def getVecVariable(name: String, length: Long, updater: Updater, formatClassName: String,
                     allowPullWithIndex: Boolean): VecVariable
}
