package com.tencent.angel.ml.algorithm.classification.lr

import com.tencent.angel.ml.algorithm.MLLearner
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.optimizer.sgd.{GradientDescent, L2LogLoss}
import com.tencent.angel.ml.algorithm.utils.ValidationUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.worker.storage.Storage
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

/**
  * Learner of logistic regression model using mini-batch gradient descent.
  *
  * @param ctx: context for each task
  */
class LRLeaner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[LRLeaner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val lr_0: Double = conf.getDouble(MLConf.ML_LEAR_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_REG_L2, MLConf.DEFAULT_ML_REG_L2)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val batchNum: Int = conf.getInt(MLConf.ML_BATCH_NUM, MLConf.DEFAULT_ML_BATCH_NUM)

  // Init LR Model
  val lrModel = new LRModel(ctx, conf)
  val weight: PSModel[TDoubleVector] = lrModel.weight

  val logLoss = new L2LogLoss(reg)

  var trainNum: Int = 0
  var validNum: Int = 0
  var sampPerBatch: Int = 0

  LOG.info("sampPerBatch=" + sampPerBatch + " right=" + trainNum / batchNum)

  /**
    * run mini-batch gradient descent LR for one epoch
    *
    * @param epoch: epoch id
    * @param trainData: trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: Storage[LabeledData]): Unit = {
    LOG.info(s"Start epoch $epoch")

    // Pull weight vector w from PS.
    lrModel.pullWeightFromPS()

    // Decay learning rate.
    val lr = lr_0 / Math.sqrt(1.0 + decay * epoch)

    // Run mini-batch gradient descent.
    GradientDescent.runMiniL2BatchSGD( trainData, lrModel.localWeight, weight, lr, batchNum,
      sampPerBatch, logLoss)

  }

  /**
    * train LR model iteratively
    *
    * @param trainData: trainning data storage
    * @param validationData: validation data storage
    */
  override def train(trainData: Storage[LabeledData], validationData: Storage[LabeledData]) {
    trainNum = trainData.getTotalElemNum
    validNum = validationData.getTotalElemNum
    sampPerBatch = (trainNum / batchNum).asInstanceOf[Int]

    LOG.info(s"#total sample=${trainNum + validNum}, #feature=$feaNum, #train sample=$trainNum, " +
      s"#batch=$batchNum, #sample per batch=$sampPerBatch, #validate sample=$validNum")

    LOG.info(s"#epoch=$epochNum, init learn rate=$lr_0, learn rate decay=$decay L2 reg=$reg")

    while (ctx.getIteration < epochNum) {
      val epoch = ctx.getIteration

      trainOneEpoch(epoch, trainData)

      lrModel.weight.clock().get()

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
      ValidationUtils.calLossAucPrecision(validationData, lrModel.localWeight, logLoss);

      // Calculate loss and precision
      //ValidationUtils.calLossPrecision(validationData, lrModel.localWeight, logLoss);

    }
  }
}