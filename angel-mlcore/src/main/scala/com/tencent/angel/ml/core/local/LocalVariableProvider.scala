package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.local.variables._
import com.tencent.angel.ml.core.network.PlaceHolder
import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}
import com.tencent.angel.ml.core.variable._
import com.tencent.angel.ml.math2.utils.RowType


class LocalVariableProvider(dataFormat: String, modelType: RowType)(
  implicit conf: SharedConf, variableManager: VariableManager) extends VariableProvider {
  override def getEmbedVariable(name: String, numRows: Long, numCols: Long, updater: Updater,
                                formatClassName: String, placeHolder: PlaceHolder, taskNum: Int = 1): EmbedVariable = {
    new LocalEmbedVariable(name, numRows.toInt, numCols, updater,
      RowTypeUtils.getDenseModelType(modelType), formatClassName, true, placeHolder)
  }

  override def getMatVariable(name: String, numRows: Long, numCols: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): MatVariable = {
    (dataFormat, allowPullWithIndex) match {
      case ("dense", true) =>
        new LocalBlasMatVariable(name, numRows.toInt, numCols, updater, modelType, formatClassName, allowPullWithIndex)
      case ("libsvm" | "dummy", true) =>
        new LocalMatVariable(name, numRows.toInt, numCols, updater, modelType, formatClassName, allowPullWithIndex)
      case (_, false) =>
        new LocalBlasMatVariable(name, numRows.toInt, numCols, updater,
          RowTypeUtils.getDenseModelType(modelType), formatClassName, allowPullWithIndex)
      case (_, true) => throw MLException("dataFormat Error!")
    }
  }

  override def getVecVariable(name: String, length: Long, updater: Updater, formatClassName: String, allowPullWithIndex: Boolean): VecVariable = {
    if (allowPullWithIndex) {
      new LocalVecVariable(name, length, updater, modelType, formatClassName, allowPullWithIndex)
    } else {
      new LocalVecVariable(name, length, updater, RowTypeUtils.getDenseModelType(modelType), formatClassName, allowPullWithIndex)
    }
  }
}
