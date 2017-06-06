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

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.optimizer.sgd.{GradientDescent, L2HingeLoss}
import com.tencent.angel.ml.utils.{DistributedLogger, ValidationUtils}
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

/**
  * Learner of support vector machine model using mini-batch gradient descent.
  *
  * @param ctx: context for each task
  */
class SVMLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[SVMLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val initLearnRate: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_REG_L2, MLConf.DEFAULT_ML_REG_L2)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_Ratio, MLConf.DEFAULT_ML_BATCH_SAMPLE_Ratio)
  val batchNum: Int = conf.getInt(MLConf.ML_SGD_BATCH_NUM, MLConf.DEFAULT_ML_SGD_BATCH_NUM)

  val svmModel = new SVMModel(ctx, conf)
  val weight:PSModel[TDoubleVector] = svmModel.weight

  // SVM used hing loss
  val hingeLoss = new L2HingeLoss(reg)

  // Logger to save logs on HDFS
  val logger = DistributedLogger(ctx)
  logger.setNames("train.loss", "validate.loss", "global.validate.loss")


  /**
    * run mini-batch gradient descent SVM for one epoch
    *
    * @param epoch: epoch id
    * @param trainData: trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], batchSize: Int): TDoubleVector = {

    // Decay learning rate.
    val learnRate = initLearnRate / Math.sqrt(1.0 + decay * epoch)

    val startGD = System.currentTimeMillis()
    // Run mini-batch gradient descent.
    val ret = GradientDescent.miniBatchGD(trainData, weight, learnRate, hingeLoss, batchSize, batchNum)
    val loss = ret._1
    val localW = ret._2

    val GDcost = System.currentTimeMillis() - startGD
    LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch mini-batch update cost$GDcost ms. batch " +
      s"loss=$loss")

    localW
  }

  /**
    * train SVM model iteratively
    *
    * @param trainData: trainning data storage
    * @param validationData: validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]):
  MLModel = {
    val trainSampNum = (trainData.getTotalElemNum * spRatio).toInt
    val batchSize = trainSampNum / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}] start SVM learner. #feature=$feaNum, "
      + s"#trainSample=${trainData.getTotalElemNum}, #validateSample=${validationData.getTotalElemNum}, "
      + s"sampleRaitoPerBatch=$spRatio, #samplePerBatch=$trainSampNum")

    LOG.info(s"Task[${ctx.getTaskIndex}] #epoch=$epochNum, initLearnRate=$initLearnRate, " +
      s"learnRateDecay=$decay, L2Reg=$reg")

    while (ctx.getIteration < epochNum) {
      val epoch = ctx.getIteration
      LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch start")

      val startTrain = System.currentTimeMillis()
      val localW = trainOneEpoch(epoch, trainData, batchSize)
      val trainCost = System.currentTimeMillis() - startTrain

      val startVali = System.currentTimeMillis()
      val valiLoss = validate(epoch, localW, trainData, validationData)
      val valiCost = System.currentTimeMillis() - startVali

      LOG.info(s"Task[${ctx.getTaskIndex}]epoch=$epoch success. train cost $trainCost ms, " +
        s"validate cost $valiCost ms")

      ctx.incIteration()
    }

    svmModel
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch          : epoch id
    * @param validationData : validata data storage
    */
  def validate(epoch: Int, weight:TDoubleVector, trainData:DataBlock[LabeledData], validationData:
    DataBlock[LabeledData]) = {
    val trainMerics = ValidationUtils.calMetrics(trainData, weight, hingeLoss)
    LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch trainData loss=${trainMerics._1} " +
      s"precision=${trainMerics._2} auc=${trainMerics._3} trueRecall=${trainMerics._4} " +
      s"falseRecall=${trainMerics._5}")

    if (validationData.getTotalElemNum > 0) {
      val valiMetric = ValidationUtils.calMetrics(validationData, weight, hingeLoss);
      LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch validationData loss=${valiMetric._1} " +
        s"precision=${valiMetric._2} auc=${valiMetric._3} trueRecall=${valiMetric._4} " +
        s"falseRecall=${valiMetric._5}")
    }
  }
}
