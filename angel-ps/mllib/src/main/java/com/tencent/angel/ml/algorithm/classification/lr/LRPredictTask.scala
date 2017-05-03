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

package com.tencent.angel.ml.algorithm.classification.lr

import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.{DataParser}
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}


class LRPredictTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, 1)
  val dataFormat: String = conf.get(MLConf.ML_DATAFORMAT)

  @throws[Exception]
  def run(ctx: TaskContext) {
    val lrmodel = new LRModel(conf)

    lrmodel.pullWeightFromPS()

    val predictResult = lrmodel.predict(trainDataStorage)
    System.out.println("predictstorage.len=" + predictResult.getTotalElemNum)
    HdfsUtil.writeStorage(predictResult, ctx)
  }

  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parse(key, value, feaNum, "predict", dataFormat)
  }
}