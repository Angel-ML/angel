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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.regression.linear

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Predict task of linear regression, first pull LinearReg weight vector from PS, second predict the
  * value of local dataset
  * @param ctx : context of current task
  */

class LinearRegPredictTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  // Number of features of model and dataset
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, 1)
  // Data format, we support libsvm and dummy
  val dataFormat: String = conf.get(MLConf.ML_DATAFORMAT)

  @throws[Exception]
  def train(ctx: TaskContext) {
    // LinearReg model, parameters set by conf, PS load weight vector form hdfs before this task runs.
    val lrmodel = new LinearRegModel(ctx,conf)

    // Pull LinearReg weight vector from PS which is save in lrmodel.localWeight
    lrmodel.weight.getRow(0);

    // Predict the input dataset and save the result
    val predictResult = lrmodel.predict(dataBlock)
    System.out.println("predict storage.len=" + predictResult.getTotalElemNum)

    HdfsUtil.writeStorage(predictResult, ctx)
  }

  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, dataFormat, false)
  }
}