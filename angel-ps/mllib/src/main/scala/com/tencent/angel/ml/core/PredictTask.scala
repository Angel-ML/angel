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

import java.io.IOException

import com.tencent.angel.exception.AngelException
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.mlcore.data.{DataParser, TransLabel}
import com.tencent.angel.ml.core.utils.SConfHelper
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.utils.HDFSUtils
import com.tencent.angel.mlcore.MLModel
import com.tencent.angel.ml.model.{OldMLModel => OldMLModel}
import com.tencent.angel.worker.task.{BaseTask, TaskContext}

abstract class PredictTask[KEYIN, VALUEIN](ctx: TaskContext)
  extends BaseTask[KEYIN, VALUEIN, LabeledData](ctx) with SConfHelper {
  val sharedConf: SharedConf = initConf(conf)
  val indexRange: Long = conf.getLong(MLCoreConf.ML_FEATURE_INDEX_RANGE, MLCoreConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  private val transLabel: TransLabel = TransLabel.get(
    sharedConf.getString(AngelMLConf.ML_DATA_LABEL_TRANS, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS),
    sharedConf.getDouble(AngelMLConf.ML_DATA_LABEL_TRANS_THRESHOLD, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD)
  )
  protected val dataParser = DataParser(sharedConf.indexRange, sharedConf.inputDataFormat,
    sharedConf.getString(AngelMLConf.ML_DATA_SPLITOR, AngelMLConf.DEFAULT_ML_DATA_SPLITOR),
    sharedConf.modelType, sharedConf.getBoolean(AngelMLConf.ML_DATA_HAS_LABEL, AngelMLConf.DEFAULT_ML_DATA_HAS_LABEL),
    false, transLabel)

  @throws(classOf[AngelException])
  final def run(taskContext: TaskContext): Unit = {
    this.predict(taskContext)
  }

  def predict(taskContext: TaskContext)

  @throws(classOf[IOException])
  protected final def predict(taskContext: TaskContext, model: MLModel, dataBlock: DataBlock[LabeledData]): Unit = {
    val predictResult = model.predict(dataBlock)
    HDFSUtils.writeStorage(predictResult, taskContext)
  }

  @throws(classOf[IOException])
  protected final def predict(taskContext: TaskContext, model: OldMLModel, dataBlock: DataBlock[LabeledData]): Unit = {
    val predictResult = model.predict(dataBlock)
    HDFSUtils.writeStorage(predictResult, taskContext)
  }
}