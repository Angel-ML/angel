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

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer.ftrl.FTRL
import com.tencent.angel.ml.optimizer.sgd.loss.L1LogLoss
import com.tencent.angel.ml.utils.ValidationUtils
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}


/**
  * A FTRL LR Learner
  *
  * @param ctx
  */
class FTRLLRLearner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[FTRLLRLearner])

  // Init LR Model
  val ftrlModel = new FTRLLRModel(conf, ctx)

  // Init ftrl algorithm parameters
  val indexRange: Long = ftrlModel.indexRange
  val epochNum: Int = ftrlModel.epochNum
  val batchSize: Int = ftrlModel.batchSize
  val spRatio: Double = ftrlModel.spRatio
  val alpha: Double = ftrlModel.alpha
  val beta: Double = ftrlModel.beta
  val lambda1: Double = ftrlModel.lambda1
  val lambda2: Double = ftrlModel.lambda2

  val l1LL = new L1LogLoss(lambda1)

  /**
    * Train a FTRL Logistic Regression Model
    *
    * @param trainData : input train data storage
    * @param valiData  : validate data storage
    * @return : a learned model
    */
  override
  def train(trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]): MLModel = {
    LOG.info("Start to train a FTRL LR Model.")
    LOG.info(s"#trainData=${trainData.size()}, #valiData=${valiData.size()}, " +
      s"#samplePerBatch=$batchSize. #feature=$indexRange, #epoch=$epochNum, " +
      s"alpha=$alpha, beta=$beta, lambda1=$lambda1, lambda2=$lambda2.")

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(valiData.size()))

    for (epoch <- 0 until epochNum) {
      LOG.info(s"Task[${ctx.getTaskId.getIndex}] Start epoch $epoch")

      val startEpoch = System.currentTimeMillis()
      val w = oneEpoch(trainData)
      val epochCost = (System.currentTimeMillis() - startEpoch) / 1000

      val startVali = System.currentTimeMillis()
      validate(epoch, w, trainData, valiData)
      val valiCost = (System.currentTimeMillis() - startVali) / 1000

      LOG.info(s"Task[${ctx.getTaskId.getIndex}] Epoch=$epoch: train cost $epochCost s, validate" +
        s" cost $valiCost s.")
    }

    ftrlModel
  }

  def oneEpoch(trainData: DataBlock[LabeledData]): TDoubleVector = {
    val weight = FTRL.ftrl(ftrlModel.zMat, ftrlModel.nMat, l1LL, trainData, batchSize, alpha,
      beta, lambda1, lambda2)

    weight
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, weight: TDoubleVector, trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]) = {
    val trainMetrics = ValidationUtils.calMetrics(trainData, weight, l1LL)
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch = $epoch " +
      s"trainData loss = ${trainMetrics._1 / trainData.size()} " +
      s"precision = ${trainMetrics._2} " +
      s"auc = ${trainMetrics._3} " +
      s"trueRecall = ${trainMetrics._4} " +
      s"falseRecall = ${trainMetrics._5}")
    globalMetrics.metric(MLConf.TRAIN_LOSS, trainMetrics._1)

    if (valiData.size > 0) {
      val validMetric = ValidationUtils.calMetrics(valiData, weight, l1LL);
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
        s"validationData loss=${validMetric._1 / valiData.size()} " +
        s"precision=${validMetric._2} " +
        s"auc=${validMetric._3} " +
        s"trueRecall=${validMetric._4} " +
        s"falseRecall=${validMetric._5}")
      globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1)
    }
  }
}
