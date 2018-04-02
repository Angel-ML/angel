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
package com.tencent.angel.ml.classification.ftrllr

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object FTRLLRModel {
  def apply(conf: Configuration) = {
    new FTRLLRModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new FTRLLRModel(conf, ctx)
  }
}

class FTRLLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[FTRLLRModel])

  val FTRL_LR_Z = "ftrl_lr_z"
  val FTRL_LR_N = "ftrl_lr_n"

  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val batchSize: Int = conf.getInt(MLConf.ML_FTRL_BATCH_SIZE, MLConf.DEFAULT_ML_FTRL_BATCH_SIZE)
  val spRatio: Double = conf.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)
  val alpha: Double = conf.getDouble(MLConf.ML_FTRL_ALPHA, MLConf.DEFAULT_ML_FTRL_ALPHA)
  val beta: Double = conf.getDouble(MLConf.ML_FTRL_BETA, MLConf.DEFAULT_ML_FTRL_BETA)
  val lambda1: Double = conf.getDouble(MLConf.ML_FTRL_LAMBDA1, MLConf.DEFAULT_ML_FTRL_LAMBDA1)
  val lambda2: Double = conf.getDouble(MLConf.ML_FTRL_LAMBDA2, MLConf.DEFAULT_ML_FTRL_LAMBDA2)


  // PSModel z, z^t = \Sigma_{s=1}^t g - \Sigma_{s=1}^t \delta^s \cdot w
  val zMat = PSModel(FTRL_LR_Z, 1, indexRange, -1, -1, modelSize)
    .setAverage(true)
    .setRowType(RowType.T_DOUBLE_SPARSE)

  // PSModel n, n^t = \Sigma_{i=1}^t g_i^2
  val nMat = PSModel(FTRL_LR_N, 1, indexRange, -1, -1, modelSize).setAverage(true)
    .setAverage(true)
    .setRowType(RowType.T_DOUBLE_SPARSE)

  addPSModel(FTRL_LR_Z, zMat)
  addPSModel(FTRL_LR_N, nMat)

  setSavePath(conf)
  setLoadPath(conf)

  /**
    *
    * @param dataSet
    * @return
    */
  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = ???
}

abstract class FTRLLRPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {

}
