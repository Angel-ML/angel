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

package com.tencent.angel.ml.classification2.lr

import java.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.optimizer2.{OptMethods, Optimizer}
import com.tencent.angel.ml.optimizer2.sgd._
import com.tencent.angel.ml.optimizer2.utils.MeasureUtils
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
  val LOG: Log = LogFactory.getLog(classOf[LRLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val batchSize: Int = conf.getInt(MLConf.ML_MINIBATCH_SIZE, MLConf.DEFAULT_ML_MINIBATCH_SIZE)
  val numUpdateEpoch: Int = conf.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO, MLConf.DEFAULT_ML_BATCH_SAMPLE_RATIO)
  val lr0: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg1: Double = conf.getDouble(MLConf.ML_LR_REG_L1, MLConf.DEFAULT_ML_LR_REG_L1)
  val reg2: Double = conf.getDouble(MLConf.ML_LR_REG_L2, MLConf.DEFAULT_ML_LR_REG_L2)
  val optMethod: String = conf.get(MLConf.ML_OPT_METHOD, MLConf.DEFAULT_ML_OPT_METHOD)

  // Init LR Model
  val lrModel = new LRModel(conf, ctx)

  val l1Reg: Map[String, Double] = if (reg1 != 0.0) {
    Map[String, Double]("lr_weight" -> reg1, "lr_intercept" -> 0.0)
  } else null

  val l2Reg: Map[String, Double] = if (reg2 != 0.0) {
    Map[String, Double]("lr_weight" -> reg2, "lr_intercept" -> 0.0)
  } else null

  val optimizer: Optimizer = optMethod match {
    case OptMethods.AdaDelta =>
      val rho = conf.getDouble(MLConf.ML_OPT_ADADELTA_RHO, MLConf.DEFAULT_ML_OPT_ADADELTA_RHO)
      new AdaDelta(batchSize, numUpdateEpoch, lr0, rho, l2Reg)
    case OptMethods.AdaGrad =>
      new AdaGrad(batchSize, numUpdateEpoch, lr0, l2Reg)
    case OptMethods.Adam =>
      val rho = conf.getDouble(MLConf.ML_OPT_ADAMM_RHO, MLConf.DEFAULT_ML_OPT_ADAMM_RHO)
      val phi = conf.getDouble(MLConf.ML_OPT_ADAMM_PHI, MLConf.DEFAULT_ML_OPT_ADAMM_PHI)
      new Adam(batchSize, numUpdateEpoch, lr0, rho, phi, l2Reg)
    case OptMethods.MiniBatchSGD =>
      new MiniBatchSGD(batchSize, numUpdateEpoch, lr0, l1Reg, l2Reg)
    case OptMethods.Momentum =>
      val rho = conf.getDouble(MLConf.ML_OPT_MOMENTUM_RHO, MLConf.DEFAULT_ML_OPT_MOMENTUM_RHO)
      new Momentum(batchSize, numUpdateEpoch, lr0, rho, l2Reg)
    case OptMethods.RMSprop =>
      val rho = conf.getDouble(MLConf.ML_OPT_RMSPROP_RHO, MLConf.DEFAULT_ML_OPT_RMSPROP_RHO)
      new RMSprop(batchSize, numUpdateEpoch, lr0, rho, l2Reg)
    case _ => throw new AngelException("Optimization method is not support!")
  }

  /**
    * run mini-batch gradient descent LR for one epoch
    *
    * @param epoch     : epoch id
    * @param trainData : trainning data storage
    */
  def trainOneEpoch[N: Numeric : TypeTag](epoch: Int, trainData: DataBlock[LabeledData], indexes: Array[N]): util.HashMap[String, TUpdate] = {
    // Decay learning rate.
    optimizer.lr = Math.max(lr0 / Math.sqrt(1.0 + decay * epoch), lr0 / 5.0)
    optimizer.epoch = epoch
    // (util.HashMap[String, TUpdate], Double)
    val result = optimizer.optimize(trainData, lrModel, indexes)

    result._1
  }

  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    train(trainData, validationData, new Array[Int](0))
  }

  def train[N: Numeric : TypeTag](trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData], indexes: Array[N]): MLModel = {

    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train the model...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr0, learnRateDecay=$decay, L1Reg=$reg1, L2Reg=$reg2")

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(validationData.size))

    lrModel.initModels(indexes)

    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val startTrain = System.currentTimeMillis()
      val localParams = trainOneEpoch(epoch, trainData, indexes)
      val trainCost = System.currentTimeMillis() - startTrain

      val startValid = System.currentTimeMillis()
      validate(epoch, localParams, trainData, validationData)
      val validCost = System.currentTimeMillis() - startValid

      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch success. " +
        s"epoch cost ${trainCost + validCost} ms." +
        s"train cost $trainCost ms. " +
        s"validation cost $validCost ms.")

      ctx.incEpoch()
    }

    lrModel
  }

  def validate(epoch: Int, params: util.HashMap[String, TUpdate], trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]): Unit = {
    val trainMetrics = MeasureUtils.calBinClassicationMetrics(trainData, 0.5, lrModel, params)
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch = $epoch " +
      s"trainData loss = ${trainMetrics._1 / trainData.size()} " +
      s"precision = ${trainMetrics._2} " +
      s"auc = ${trainMetrics._3} " +
      s"trueRecall = ${trainMetrics._4} " +
      s"falseRecall = ${trainMetrics._5}")
    globalMetrics.metric(MLConf.TRAIN_LOSS, trainMetrics._1)

    if (valiData.size > 0) {
      val validMetric = MeasureUtils.calBinClassicationMetrics(valiData, 0.5, lrModel, params)
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch = $epoch " +
        s"validationData loss = ${validMetric._1 / valiData.size()} " +
        s"precision = ${validMetric._2} " +
        s"auc = ${validMetric._3} " +
        s"trueRecall = ${validMetric._4} " +
        s"falseRecall = ${validMetric._5}")
      globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1)
    }
  }
}
