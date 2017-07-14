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

package com.tencent.angel.ml.classification.lr

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer.sgd.{GradientDescent, L2LogLoss}
import com.tencent.angel.ml.utils.{DistributedLogger, ValidationUtils}
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

/**
  * Learner of logistic regression model using mini-batch gradient descent.
  *
  * @param ctx: context for each task
  */
class LRLearner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[LRLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val lr_0: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_REG_L2, MLConf.DEFAULT_ML_REG_L2)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_Ratio, MLConf.DEFAULT_ML_BATCH_SAMPLE_Ratio)
  val batchNum: Int = conf.getInt(MLConf.ML_SGD_BATCH_NUM, MLConf.DEFAULT_ML_SGD_BATCH_NUM)

  // Init LR Model
  val lrModel = new LRModel(ctx, conf)
  // LR uses log loss
  val logLoss = new L2LogLoss(reg)

  // Logger to save logs on HDFS
  val logger = DistributedLogger(ctx)

  logger.setNames("train.loss", "train.precision", "train.auc", "train.trueRecall", "train" +
    ".falseRecall", "vali.loss", "vali.precision", "vali.auc", "vali.trueRecall", "vali" +
    ".falseRecall")

  /**
    * run mini-batch gradient descent LR for one epoch
    *
    * @param epoch: epoch id
    * @param trainData: trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], batchSize: Int): TDoubleVector
  = {
    // Decay learning rate.
    val lr = lr_0 / Math.sqrt(1.0 + decay * epoch)

    // Apply mini-batch gradient descent
    val startBatch = System.currentTimeMillis()
    val batchGD = GradientDescent.miniBatchGD(trainData, lrModel.weight, None,
                                              lr, logLoss, batchSize, batchNum)
    val loss = batchGD._1
    val localWeight = batchGD._2
    val batchCost = System.currentTimeMillis() - startBatch
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch mini-batch update success. cost $batchCost" +
      s" ms. " +
      s"batch loss = $loss")

    localWeight
  }

  /**
    * train LR model iteratively
    *
    * @param trainData: trainning data storage
    * @param validationData: validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    val trainSampleSize = (trainData.getTotalElemNum * spRatio).toInt
    val samplePerBatch = trainSampleSize / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train a LR model...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: Sample Ratio per Batch=$spRatio, Sample Size Per $samplePerBatch")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr_0, learnRateDecay=$decay, L2Reg=$reg")

    while (ctx.getIteration < epochNum) {
      val epoch = ctx.getIteration
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val startTrain = System.currentTimeMillis()
      val localWeight = trainOneEpoch(epoch, trainData, samplePerBatch)
      val trainCost = System.currentTimeMillis() - startTrain

      val startVali = System.currentTimeMillis()
      validate(epoch, localWeight, trainData, validationData)
      val valiCost = System.currentTimeMillis() - startVali

      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch success. epoch cost " +
        s"${trainCost+valiCost} ms. train cost $trainCost ms. validation cost $valiCost ms.")

      ctx.incIteration()
    }

    logger.close()
    lrModel
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch: epoch id
    * @param validationData: validata data storage
    */
  def validate(epoch: Int, weight: TDoubleVector, trainData:DataBlock[LabeledData], validationData:
  DataBlock[LabeledData]) = {

    val trainMerics = ValidationUtils.calMetrics(trainData, weight, logLoss)
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch trainData loss=${trainMerics._1} " +
      s"precision=${trainMerics._2} auc=${trainMerics._3} trueRecall=${trainMerics._4} " +
      s"falseRecall=${trainMerics._5}")

    if (validationData.getTotalElemNum > 0) {
      val valiMetric = ValidationUtils.calMetrics(validationData, weight, logLoss);
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch validationData loss=${valiMetric._1} " +
        s"precision=${valiMetric._2} auc=${valiMetric._3} trueRecall=${valiMetric._4} " +
        s"falseRecall=${valiMetric._5}")

    logger.addValues(trainMerics._1, trainMerics._2, trainMerics._3, trainMerics._4, trainMerics
      ._5, valiMetric._1, valiMetric._2, valiMetric._3, valiMetric._4, valiMetric._5)
    }
  }

}
