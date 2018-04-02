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

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{TDoubleVector, TIntDoubleVector}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer.sgd.GradientDescent
import com.tencent.angel.ml.optimizer.sgd.loss.SquareL2Loss
import com.tencent.angel.ml.utils.ValidationUtils
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

/**
  * Learner of linear regression model using mini-batch gradient descent.
  *
  * @param ctx : context for each task
  */
class LinearRegLeaner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[LinearRegLeaner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val initLearnRate: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_LR_REG_L2, MLConf.DEFAULT_ML_LR_REG_L2)
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO, MLConf.DEFAULT_ML_BATCH_SAMPLE_RATIO)
  val batchNum: Int = conf.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)

  val model = new LinearRegModel(conf, ctx)

  val squareLoss = new SquareL2Loss(reg)

  /**
    * run mini-batch gradient descent LinearReg for one epoch
    *
    * @param epoch     : epoch id
    * @param trainData : trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], batchSize: Int): TDoubleVector = {
    LOG.info(s"Start epoch $epoch")

    // Decay learning rate.
    val learnRate = initLearnRate / Math.sqrt(1.0 + decay * epoch)

    // Run mini-batch gradient descent.
    val ret = GradientDescent.miniBatchGD(trainData, model.weight, None,
      learnRate, squareLoss, batchSize, batchNum, ctx)

    val loss = ret._1
    val localW = ret._2

    localW
  }

  /**
    * train LinearReg model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]):
  MLModel = {
    val trainSampNum = (trainData.size * spRatio).toInt
    val batchSize = trainSampNum / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}] start to train a linear regression model. " +
      s"#feature=$indexRange, #trainSample=${trainData.size}, #validateSample=${validationData.size}, "
      + s"sampleRaitoPerBatch=$spRatio, #samplePerBatch=$batchSize")

    LOG.info(s"Task[${ctx.getTaskIndex}] #epoch=$epochNum, initLearnRate=$initLearnRate, " +
      s"learnRateDecay=$decay, L2Reg=$reg")


    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch

      val localW = trainOneEpoch(epoch, trainData, batchSize)

      validate(epoch, localW, validationData)

      ctx.incEpoch()
    }

    model
  }

  /**
    * validate MSE, RMSE, MAE and R2
    *
    * @param epoch          : epoch id
    * @param validationData : validata data storage
    */
  def validate(epoch: Int, weight: TDoubleVector, validationData: DataBlock[LabeledData]): Unit = {
    if (validationData.size > 0) {
      ValidationUtils.calMSER2(validationData, weight, squareLoss)
    }
  }

}
