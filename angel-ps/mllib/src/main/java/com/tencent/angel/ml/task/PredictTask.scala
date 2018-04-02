/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.ml.task

import java.io.IOException

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.{BaseTask, TaskContext}

abstract class PredictTask[KEYIN, VALUEIN](ctx: TaskContext) extends BaseTask[KEYIN, VALUEIN, LabeledData](ctx) {
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val dataParser = DataParser(conf)

  @throws(classOf[AngelException])
  final def run(taskContext: TaskContext) {
    this.predict(taskContext)
  }

  def predict(taskContext: TaskContext)

  @throws(classOf[IOException])
  protected final def predict(taskContext: TaskContext, model: MLModel, dataBlock: DataBlock[LabeledData]) {
    val predictResult = model.predict(dataBlock)
    HdfsUtil.writeStorage(predictResult, taskContext)
  }
}
