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

package com.tencent.angel.ml.algorithm.classification.svm

import com.tencent.angel.ml.algorithm.MLLearner
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.optimizer.sgd.{GradientDescent, L2HingeLoss}
import com.tencent.angel.ml.algorithm.utils.ValidationUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.worker.storage.Storage
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

/**
  * Learner of support vector machine model using mini-batch gradient descent.
  *
  * @param ctx: context for each task
  */
class SVMLeaner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[SVMLeaner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val initLearnRate: Double = conf.getDouble(MLConf.ML_LEAR_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_REG_L2, MLConf.DEFAULT_ML_REG_L2)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val batchNum: Int = conf.getInt(MLConf.ML_BATCH_NUM, MLConf.DEFAULT_ML_BATCH_NUM)

  var sampPerBatch = 0
  var trainNum = 0
  var validNum = 0

  val modelPath = conf.get(MLConf.ANGEL_MODEL_PATH)

  val svmModel = new SVMModel(ctx, conf)
  val weight:PSModel[TDoubleVector] = svmModel.weight

  val hingeLoss = new L2HingeLoss(reg)

  /**
    * run mini-batch gradient descent SVM for one epoch
    *
    * @param epoch: epoch id
    * @param trainData: trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: Storage[LabeledData]): Unit = {
    LOG.info(s"Start epoch $epoch")

    // Pull weight vector w from PS.
    svmModel.pullWeightFromPS()

    // Decay learning rate.
    val learnRate = initLearnRate / Math.sqrt(1.0 + decay * epoch)

    // Run mini-batch gradient descent.
    GradientDescent.runMiniL2BatchSGD(trainData,
      svmModel.localWeight, weight,
      learnRate, batchNum, sampPerBatch, hingeLoss)
  }

  /**
    * train SVM model iteratively
    *
    * @param trainData: trainning data storage
    * @param validationData: validation data storage
    */
  override def train(trainData: Storage[LabeledData], validationData: Storage[LabeledData]) {

    trainNum = trainData.getTotalElemNum
    validNum = validationData.getTotalElemNum
    sampPerBatch = trainNum / batchNum
    LOG.info(s"#total sample=${trainNum + validNum}, #feature=$feaNum, #train sample=$trainNum, " +
      s"#batch=$batchNum, #sample per batch=$sampPerBatch, #validate sample=$validNum")

    LOG.info(s"#epoch=$epochNum, init learn rate=$initLearnRate, learn rate decay=$decay L2 reg=$reg")

    while (ctx.getIteration < epochNum) {
      val epoch = ctx.getIteration

      trainOneEpoch(epoch, trainData)

      svmModel.weight.clock().get()

      validate(epoch, validationData)

      ctx.increaseIteration()
    }

  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch: epoch id
    * @param validationData: validata data storage
    */
  def validate(epoch: Int, validationData: Storage[LabeledData]): Unit = {

    if (validNum > 0) {
      // Calculate loss, AUC and precision
      ValidationUtils.calLossAucPrecision(validationData, svmModel.localWeight, hingeLoss)
    }
  }

}