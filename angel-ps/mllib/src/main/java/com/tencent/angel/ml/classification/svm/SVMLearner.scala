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
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer.sgd.GradientDescent
import com.tencent.angel.ml.optimizer.sgd.loss.L2HingeLoss
import com.tencent.angel.ml.utils.ValidationUtils
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

/**
  * Learner of support vector machine model using mini-batch gradient descent.
  *
  * @param ctx : context for each task
  */
class SVMLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[SVMLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val initLearnRate: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_LR_REG_L2, MLConf.DEFAULT_ML_LR_REG_L2)
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO, MLConf.DEFAULT_ML_BATCH_SAMPLE_RATIO)
  val batchNum: Int = conf.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)

  val svmModel = new SVMModel(conf, ctx)

  // SVM used hing loss
  val hingeLoss = new L2HingeLoss(reg)


  /**
    * run mini-batch gradient descent SVM for one epoch
    *
    * @param epoch     : epoch id
    * @param trainData : trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], batchSize: Int): TDoubleVector = {

    // Decay learning rate.
    val learnRate = initLearnRate / Math.sqrt(1.0 + decay * epoch)

    val startGD = System.currentTimeMillis()
    // Run mini-batch gradient descent.
    val ret = GradientDescent.miniBatchGD(trainData, svmModel.weight, None,
      learnRate, hingeLoss, batchSize, batchNum, ctx)
    val loss = ret._1
    val localW = ret._2

    val GDCost = System.currentTimeMillis() - startGD
    LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch mini-batch update cost$GDCost ms. loss=$loss")

    localW
  }

  /**
    * train SVM model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]):
  MLModel = {
    val trainSampNum = (trainData.size * spRatio).toInt
    val batchSize = trainSampNum / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}] start SVM learner. #feature=$indexRange, "
      + s"#trainSample=${trainData.size}, #validateSample=${validationData.size}, "
      + s"sampleRaitoPerBatch=$spRatio, #samplePerBatch=$trainSampNum")

    LOG.info(s"Task[${ctx.getTaskIndex}] #epoch=$epochNum, initLearnRate=$initLearnRate, " +
      s"learnRateDecay=$decay, L2Reg=$reg")

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(validationData.size))

    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch start")

      val startTrain = System.currentTimeMillis()
      val localW = trainOneEpoch(epoch, trainData, batchSize)
      val trainCost = System.currentTimeMillis() - startTrain

      val startVali = System.currentTimeMillis()
      val valiLoss = validate(epoch, localW, trainData, validationData)
      val valiCost = System.currentTimeMillis() - startVali

      LOG.info(s"Task[${ctx.getTaskIndex}]epoch=$epoch success. train cost $trainCost ms, " +
        s"validate cost $valiCost ms")

      ctx.incEpoch()
    }

    svmModel
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, weight: TDoubleVector, trainData: DataBlock[LabeledData], valiData:
  DataBlock[LabeledData]) = {
    val trainMetrics = ValidationUtils.calMetrics(trainData, weight, hingeLoss)
    LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch trainData loss=${trainMetrics._1 / trainData.size()} " +
      s"precision=${trainMetrics._2} auc=${trainMetrics._3} trueRecall=${trainMetrics._4} " +
      s"falseRecall=${trainMetrics._5}")
    globalMetrics.metric(MLConf.TRAIN_LOSS, trainMetrics._1)

    if (valiData.size > 0) {
      val valiMetric = ValidationUtils.calMetrics(valiData, weight, hingeLoss);
      LOG.info(s"Task[${ctx.getTaskIndex}] epoch=$epoch validationData loss=${
        valiMetric._1 /
          valiData.size()
      }" + s"precision=${valiMetric._2} auc=${valiMetric._3} trueRecall=${valiMetric._4} " +
        s"falseRecall=${valiMetric._5}")
      globalMetrics.metric(MLConf.VALID_LOSS, valiMetric._1)
    }
  }
}
