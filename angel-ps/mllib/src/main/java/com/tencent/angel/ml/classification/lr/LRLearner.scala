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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer.sgd.GradientDescent
import com.tencent.angel.ml.optimizer.sgd.loss.{L1LogLoss, L2LogLoss, Loss}
import com.tencent.angel.ml.utils.ValidationUtils
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.math.Numeric
import scala.reflect.runtime.universe._

/**
  * Learner of logistic regression model using mini-batch gradient descent.
  *
  */
class LRLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOSS1 = "loss1"
  val LOSS2 = "loss2"
  val LOG: Log = LogFactory.getLog(classOf[LRLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val lr_0: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg1: Double = conf.getDouble(MLConf.ML_LR_REG_L1, MLConf.DEFAULT_ML_LR_REG_L1)
  val reg2: Double = conf.getDouble(MLConf.ML_LR_REG_L2, MLConf.DEFAULT_ML_LR_REG_L2)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO, MLConf.DEFAULT_ML_BATCH_SAMPLE_RATIO)
  val batchNum: Int = conf.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)
  val regLoss: String = conf.getStrings(MLConf.ML_REG_LOSS_TYPE, MLConf.DEFAULT_ML_REG_LOSS_TYPE)(0)
  // control the lower bound of learning rate
  val lrLowerBound: Int = conf.getInt(MLConf.ML_LEARN_RATE_BOUND, MLConf.DEFAULT_ML_LEARN_RATE_BOUND)
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)

  // Init LR Model
  val lrModel = new LRModel(conf, ctx)

  LOG.info("the loss is:" + regLoss)
  // LR uses log loss
  val regLL = regLoss match {
    case LOSS1 => new L1LogLoss(reg1)
    case LOSS2 => new L2LogLoss(reg2)
    case _ => new L2LogLoss(reg2)
  }

  /**
    * run mini-batch gradient descent LR for one epoch
    *
    * @param epoch     : epoch id
    * @param trainData : trainning data storage
    */
  def trainOneEpoch[N: Numeric : TypeTag](epoch: Int,
                                          trainData: DataBlock[LabeledData],
                                          batchSize: Int,
                                          indexes: Array[N],
                                          ctx: TaskContext): TDoubleVector = {
    val LLoss: Loss = regLL

    // Decay learning rate.
    var lr = lr_0 / Math.sqrt(1.0 + decay * epoch)

    lr = Math.max(lr_0 * (1.0 / lrLowerBound), lr)
    LOG.info("the current learning rate is:" + lr)

    // Apply mini-batch gradient descent
    val startBatch = System.currentTimeMillis()
    if (lrModel.intercept.isDefined) {
      LOG.info("the intercept is:" + lrModel.intercept.get.getRow(0).asInstanceOf[TDoubleVector].get(0))
    } else {
      LOG.info("intercept is not set")
    }


    val elementType = typeOf[N]
    val batchGD = elementType match {
      case t if t == typeOf[Int] => GradientDescent.miniBatchGD(trainData,
        lrModel.weight,
        lrModel.intercept,
        lr,
        LLoss,
        batchSize,
        batchNum,
        indexes.asInstanceOf[Array[Int]],
        ctx)
      case t if t == typeOf[Long] => GradientDescent.miniBatchGD(trainData,
        lrModel.weight,
        lrModel.intercept,
        lr,
        LLoss,
        batchSize,
        batchNum,
        indexes.asInstanceOf[Array[Long]],
        ctx)
      case _ => throw new AngelException(s"unsupported type: $elementType")
    }
    val loss = batchGD._1
    val localWeight = batchGD._2

    val batchCost = System.currentTimeMillis() - startBatch
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch mini-batch update success." +
      s"Cost $batchCost ms. " +
      s"Batch loss = $loss")
    localWeight
  }

  /**
    * train LR model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    train(trainData, validationData, new Array[Int](0))
  }

  def train[N: Numeric : TypeTag](trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData], indexes: Array[N]): MLModel = {

    // 总batch的样本量
    val trainSampleSize = (trainData.size * spRatio).toInt
    // 每个minibatch的样本量
    val samplePerBatch = trainSampleSize / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train a LR model...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: Sample Ratio per Batch=$spRatio, Sample Size Per " + s"$samplePerBatch")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr_0, " + s"learnRateDecay=$decay, Reg1=$reg1, Reg2=$reg2")

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(validationData.size))

    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val startTrain = System.currentTimeMillis()
      val localWeight = trainOneEpoch(epoch, trainData, samplePerBatch, indexes, ctx)
      val trainCost = System.currentTimeMillis() - startTrain

      val startValid = System.currentTimeMillis()
      validate(epoch, localWeight, trainData, validationData)
      val validCost = System.currentTimeMillis() - startValid

      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch success. " +
        s"epoch cost ${trainCost + validCost} ms." +
        s"train cost $trainCost ms. " +
        s"validation cost $validCost ms.")

      ctx.incEpoch()
    }

    lrModel
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, weight: TDoubleVector, trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]) = {
    val trainMetrics = ValidationUtils.calMetrics(trainData, weight, regLL)
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch = $epoch " +
      s"trainData loss = ${trainMetrics._1 / trainData.size()} " +
      s"precision = ${trainMetrics._2} " +
      s"auc = ${trainMetrics._3} " +
      s"trueRecall = ${trainMetrics._4} " +
      s"falseRecall = ${trainMetrics._5}")
    globalMetrics.metric(MLConf.TRAIN_LOSS, trainMetrics._1)

    if (valiData.size > 0) {
      val validMetric = ValidationUtils.calMetrics(valiData, weight, regLL)
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
        s"validationData loss=${validMetric._1 / valiData.size()} " +
        s"precision=${validMetric._2} " +
        s"auc=${validMetric._3} " +
        s"trueRecall=${validMetric._4} " +
        s"falseRecall=${validMetric._5}")
      globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1)
    }
  }

  def sparsity(weight: TDoubleVector, dim: Int): Double = {
    weight.nonZeroNumber().toDouble / modelSize
  }

}
