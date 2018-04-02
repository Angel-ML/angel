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
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.regression.linear.LinearRegModel._
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object LinearRegModel {
  val LR_WEIGHT_MAT = "lr_weight"
}

class LinearRegModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[LinearRegModel])


  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val weight = PSModel(LR_WEIGHT_MAT, 1, indexRange).setAverage(true)
  addPSModel(LR_WEIGHT_MAT, weight)

  super.setSavePath(conf)
  super.setLoadPath(conf)


  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val predict = new MemoryDataBlock[PredictResult](-1)
    val wVector = weight.getRow(0)
    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val id = instance.getY
      val pre = wVector.dot(instance.getX)

      predict.put(new LinearRegPredictResult(id, pre))
    }
    predict
  }
}

class LinearRegPredictResult(id: Double, dot: Double) extends PredictResult {
  override def getText(): String = {
    (id + separator + format.format(dot))
  }

}

