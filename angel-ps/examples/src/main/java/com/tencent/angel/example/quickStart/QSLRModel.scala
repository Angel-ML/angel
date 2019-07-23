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


package com.tencent.angel.example.quickStart

import com.tencent.angel.ml.core.{MLModel, PredictResult}
import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.servingmath2.utils.{LabeledData, RowType}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class QSLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  val N: Int = conf.getInt(MLCoreConf.ML_FEATURE_INDEX_RANGE, MLCoreConf.DEFAULT_ML_FEATURE_INDEX_RANGE)

  val weight = PSModel("qs.lr.weight", 1, N).setRowType(RowType.T_DOUBLE_DENSE).setAverage(true)
  addPSModel(weight)

  setSavePath(conf)
  setLoadPath(conf)

  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???
}
