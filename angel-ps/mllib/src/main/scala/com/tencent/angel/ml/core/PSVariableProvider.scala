package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}
import com.tencent.angel.ml.core.variable._
import com.tencent.angel.ml.math2.utils.RowType

class PSVariableProvider(dataFormat: String, modelType: RowType, placeHolder: PlaceHolder)(
  implicit variableManager: VariableManager) extends VariableProvider {
  private val validIndexNum: Long = SharedConf.modelSize

  override def getEmbedVariable(name: String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, taskNum: Int): EmbedVariable = {
    new PSEmbedVariable(name, numCols.toInt, numRows, validIndexNum, updater, modelType,
      formatClassName, true, taskNum, placeHolder)
  }

  override def getMatVariable(name: String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable = {
    (dataFormat, allowPullWithIndex) match {
      case ("dense", true) =>
        new PSBlasMatVariable(name, numRows.toInt, numCols, updater, modelType, formatClassName, allowPullWithIndex)
      case ("libsvm" | "dummy", true) =>
        new PSMatVariable(name, numRows.toInt, numCols, validIndexNum, updater, modelType, formatClassName, allowPullWithIndex)
      case (_, false) =>
        new PSBlasMatVariable(name, numRows.toInt, numCols, updater,
          RowTypeUtils.getDenseModelType(modelType), formatClassName, allowPullWithIndex)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable = {
    if (allowPullWithIndex) {
      new PSVecVariable(name, length, validIndexNum, updater, modelType, formatClassName, true)
    } else {
      new PSVecVariable(name, length, length, updater, RowTypeUtils.getDenseModelType(modelType), formatClassName, false)
    }
  }
}
