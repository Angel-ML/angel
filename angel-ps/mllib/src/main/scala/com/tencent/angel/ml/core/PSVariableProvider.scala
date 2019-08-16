/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.core

import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.network.PlaceHolder
import com.tencent.angel.mlcore.utils.{MLException, RowTypeUtils}
import com.tencent.angel.ml.core.variable._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.mlcore.variable._

class PSVariableProvider(dataFormat: String, sharedConf: SharedConf)(
  implicit conf: SharedConf, variableManager: VariableManager, cilsImpl: CILSImpl) extends VariableProvider {
  lazy val modelType: RowType = sharedConf.modelType
  private val validIndexNum: Long = conf.modelSize

  override def getEmbedVariable(name: String, numRows: Long, numCols: Long, updater: Updater,
                                formatClassName: String, placeHolder: PlaceHolder, taskNum: Int): EmbedVariable = {
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
