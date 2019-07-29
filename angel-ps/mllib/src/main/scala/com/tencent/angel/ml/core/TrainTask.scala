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
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.mlcore.data.{DataParser, TransLabel}
import com.tencent.angel.ml.core.utils.SConfHelper
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.worker.task.{BaseTask, TaskContext}


/**
  * The type labeled base task.
  */
abstract class TrainTask[KEYIN, VALUEIN](taskContext: TaskContext)
  extends BaseTask[KEYIN, VALUEIN, LabeledData](taskContext) with SConfHelper {
  val sharedConf: SharedConf = initConf(conf)
  private val transLabel: TransLabel = TransLabel.get(
    sharedConf.getString(AngelMLConf.ML_DATA_LABEL_TRANS, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS),
    sharedConf.getDouble(AngelMLConf.ML_DATA_LABEL_TRANS_THRESHOLD, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD)
  )
  protected val dataParser = DataParser(sharedConf.indexRange, sharedConf.inputDataFormat,
    sharedConf.getString(AngelMLConf.ML_DATA_SPLITOR, AngelMLConf.DEFAULT_ML_DATA_SPLITOR),
    sharedConf.modelType, sharedConf.getBoolean(AngelMLConf.ML_DATA_HAS_LABEL, AngelMLConf.DEFAULT_ML_DATA_HAS_LABEL),
    true, transLabel)

  final def run(taskContext: TaskContext): Unit = {
    this.train(taskContext)
  }

  def train(taskContext: TaskContext)
}
