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

package com.tencent.angel.ml.classification.svm

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TIntDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object SVMModel {
  def apply(conf: Configuration) = {
    new SVMModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new SVMModel(conf, ctx)
  }
}

class SVMModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  type V = TIntDoubleVector

  var LOG = LogFactory.getLog(classOf[SVMModel])

  val SVM_WEIGHT_MAT = "svm.weight.mat"
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)
  val weight = new PSModel(SVM_WEIGHT_MAT, 1, indexRange, -1, -1, modelSize).setAverage(true).setRowType(RowType.T_DOUBLE_DENSE)

  addPSModel(SVM_WEIGHT_MAT, weight)

  setSavePath(conf)
  setLoadPath(conf)


  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val predict = new MemoryDataBlock[PredictResult](-1)
    val wVector = weight.getRow(0)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val id = instance.getY

      var pre = 1.0
      if (wVector.dot(instance.getX) < 0)
        pre = -1.0
      predict.put(new SVMPredictResult(id, pre))
    }
    predict
  }
}

class SVMPredictResult(id: Double, pre: Double) extends PredictResult {
  override def getText(): String = {
    (id + separator + format.format(pre))
  }
}
