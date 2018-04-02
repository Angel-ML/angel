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

package com.tencent.angel.ml.classification.mlr

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TAbstractVector
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleSortedVector,
TIntDoubleVector}
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.util.Random

/**
  * Learner of MLR model using mini-batch gradient descent
  *
  */

case class mlrWeight(sigmoid_wVecot: Array[DenseDoubleVector], sigmoid_b: Array[Double],
                     softmax_wVecot: Array[DenseDoubleVector], softmax_b: Array[Double]) {

}

class MLRLearner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[MLRLearner])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val lr_0: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val decay: Double = conf.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  val reg: Double = conf.getDouble(MLConf.ML_LR_REG_L2, MLConf.DEFAULT_ML_LR_REG_L2)
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val spRatio: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO, MLConf
    .DEFAULT_ML_BATCH_SAMPLE_RATIO)
  val batchNum: Int = conf.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)

  // Number of local regions
  val rank: Int = conf.getInt(MLConf.ML_MLR_RANK, MLConf.DEFAULT_ML_MLR_RANK)
  // Init value parameter, the standard deviation of Gaussian distribution
  val vInit: Double = conf.getDouble(MLConf.ML_MLR_V_INIT, MLConf.DEFAULT_ML_MLR_V_INIT)

  // Init MLR Model
  val mlrModel = new MLRModel(conf, ctx)

  /**
    * run mini-batch gradient descent MLR for one epoch
    *
    * @param epoch     : epoch id
    * @param trainData : trainning data storage
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], batchSize: Int) = {

    // Decay learning rate.
    val lr = lr_0 / Math.sqrt(1.0 + decay * epoch)

    // Apply mini-batch gradient descent
    val startBatch = System.currentTimeMillis()
    val batchGD = miniBatchGD(trainData, lr, batchSize)
    val loss = batchGD._1
    val localWeight = batchGD._2
    val batchCost = System.currentTimeMillis() - startBatch
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch mini-batch update success." +
      s"Cost $batchCost ms. " +
      s"Batch loss = $loss")
    localWeight
  }


  /**
    * run mini-batch gradient descent MLR
    *
    * @param trainData : trainning data storage
    * @param lr        : learning rate
    * @param batchSize : number of samples per mini-batch
    */
  def miniBatchGD[M <: TIntDoubleVector](trainData: DataBlock[LabeledData],
                                         lr: Double,
                                         batchSize: Int) = {

    //Pull model from PS Server
    val (sigmoid_wVecot, sigmoid_b, softmax_wVecot, softmax_b) = mlrModel.pullFromPs()

    var totalLoss = 0.0

    val taskContext = mlrModel.sigmoid_weight.getTaskContext

    for (batch: Int <- 1 to batchNum) {
      val batchStartTs = System.currentTimeMillis()
      val grad_sigmoid_wVecot = new Array[DenseDoubleVector](rank)
      val grad_sigmoid_b = new Array[Double](rank)
      val grad_softmax_wVecot = new Array[DenseDoubleVector](rank)
      val grad_softmax_b = new Array[Double](rank)
      (0 until rank).foreach(i => {
        grad_sigmoid_wVecot(i) = new DenseDoubleVector(indexRange.toInt)
        grad_softmax_wVecot(i) = new DenseDoubleVector(indexRange.toInt)
      })

      var batchLoss: Double = 0.0

      (0 until batchSize).foreach(_ => {
        // p(y=1|x) = \Sigma^m_{i=1} \frac{exp(u_i^Tx)}{\Sigma_{j=1}^m exp(u_j^Tx)}
        // \cdot \frac{1}{1+exp(-w_i^Tx)}
        val (x: TAbstractVector, y: Double) = loopingData(trainData)
        val softmax = (0 until rank).map(i => softmax_wVecot(i).dot(x) + softmax_b(i)).toArray
        Maths.softmax(softmax)
        val sigmoid = (0 until rank).map(i => Maths.sigmoid({
          var temp = sigmoid_wVecot(i).dot(x) + sigmoid_b(i)
          temp = math.max(temp, -18)
          temp = math.min(temp, 18)
          temp
        })).toArray
        val pre = (0 until rank).map(i => softmax(i) * sigmoid(i)).sum

        val loss = {
          if (y == 1) -Math.log(pre)
          else -Math.log(1 - pre)
        }
        batchLoss += loss

        // Update sigmoid parameters
        (0 until rank).map(i => {
          var temp = softmax(i) * sigmoid(i) * (1 - sigmoid(i)) * y
          if (y == 1) {
            temp /= (-pre)
          } else {
            temp /= (pre - 1)
          }
          grad_sigmoid_b(i) += temp
          grad_sigmoid_wVecot(i).plusBy(x, temp)
        })
        (0 until rank).map(i => {
          var temp = {
            if (y == 1) {
              softmax(i) * (1 - sigmoid(i) / pre)
            } else {
              softmax(i) * (1 - (1 - sigmoid(i)) / (1 - pre))
            }
          }
          grad_softmax_b(i) += temp
          grad_softmax_wVecot(i).plusBy(x, temp)
        })
      })

      // Update softmax parameters
      grad_sigmoid_wVecot.foreach(grad => grad.timesBy(1.toDouble / batchSize.asInstanceOf[Double]))
      grad_softmax_wVecot.foreach(grad => grad.timesBy(1.toDouble / batchSize.asInstanceOf[Double]))
      (0 until rank).map(i => {
        grad_sigmoid_b(i) /= batchSize
        grad_softmax_b(i) /= batchSize
        grad_sigmoid_wVecot(i).plusBy(sigmoid_wVecot(i), reg)
        grad_softmax_wVecot(i).plusBy(softmax_wVecot(i), reg)
      })

      // Update grad to local model and push grad to PS
      val bUpdater1 = new DenseDoubleVector(1)
      bUpdater1.setRowId(0)
      val bUpdater2 = new DenseDoubleVector(1)
      bUpdater2.setRowId(0)
      (0 until rank).map(i => {
        sigmoid_wVecot(i).plusBy(grad_sigmoid_wVecot(i), -1.0 * lr)
        softmax_wVecot(i).plusBy(grad_softmax_wVecot(i), -1.0 * lr)
        sigmoid_b(i) -= lr * grad_sigmoid_b(i)
        softmax_b(i) -= lr * grad_softmax_b(i)

        mlrModel.sigmoid_weight.increment(i, grad_sigmoid_wVecot(i).times(-1.0 * lr))
        mlrModel.softmax_weight.increment(i, grad_softmax_wVecot(i).times(-1.0 * lr))

        bUpdater1.set(0, -lr * grad_sigmoid_b(i))
        mlrModel.sigmoid_intercept.increment(i, bUpdater1)
        bUpdater2.set(0, -lr * grad_softmax_b(i))
        mlrModel.softmax_intercept.increment(i, bUpdater2)
      })


      totalLoss += batchLoss
      LOG.debug(s"Batch[$batch] loss = $batchLoss")
      taskContext.updateProfileCounter(batchSize, (System.currentTimeMillis() - batchStartTs).toInt)
    }

    totalLoss /= (batchSize * batchNum)

    totalLoss += {
      (0 until rank).map(i => {
        sigmoid_wVecot(i).dot(sigmoid_wVecot(i)) + softmax_wVecot(i).dot(softmax_wVecot(i))
      }).sum * 0.5 * reg
    }

    val f1 = mlrModel.sigmoid_weight.clock()
    val f2 = mlrModel.softmax_weight.clock()
    val f3 = mlrModel.sigmoid_intercept.clock()
    val f4 = mlrModel.softmax_intercept.clock()

    f1.get()
    f2.get()
    f3.get()
    f4.get()

    (totalLoss, mlrWeight(sigmoid_wVecot, sigmoid_b, softmax_wVecot, softmax_b))
  }


  /**
    * Read LabeledData from DataBlock Looping. If it reach the end, start from the beginning again.
    *
    * @param trainData
    * @return
    */
  def loopingData(trainData: DataBlock[LabeledData]): (TAbstractVector, Double) = {
    var data = trainData.read()
    if (data == null) {
      trainData.resetReadIndex()
      data = trainData.read()
    }

    if (data != null)
      (data.getX, data.getY)
    else
      throw new AngelException("Train data storage is empty or corrupted.")
  }

  /**
    * train MLR model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]):
  MLRModel = {
    val trainSampleSize = (trainData.size * spRatio).toInt
    val samplePerBatch = trainSampleSize / batchNum

    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train a MLR model...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: Sample Ratio per Batch=$spRatio, Sample Size Per " +
      s"$samplePerBatch")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr_0, " +
      s"learnRateDecay=$decay, L2Reg=$reg")

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(1))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(1))

    val beforeInit = System.currentTimeMillis()
    initModels()
    val initCost = System.currentTimeMillis() - beforeInit
    LOG.info(s"Init matrixes cost $initCost ms.")

    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val startTrain = System.currentTimeMillis()
      val localWeight = trainOneEpoch(epoch, trainData, samplePerBatch)
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

    mlrModel
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, weight: mlrWeight, trainData: DataBlock[LabeledData], valiData:
  DataBlock[LabeledData]) = {
    val trainLoss = evaluate(trainData, weight)
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch = $epoch " +
      s"trainData loss = ${trainLoss} ")
    globalMetrics.metric(MLConf.TRAIN_LOSS, trainLoss)

    if (valiData.size > 0) {
      val validLoss = evaluate(valiData, weight)
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
        s"validationData loss=${validLoss} ")
      globalMetrics.metric(MLConf.VALID_LOSS, validLoss)
    }
  }


  def evaluate(dataBlock: DataBlock[LabeledData], weight: mlrWeight): Double = {
    var loss = 0.0
    val (sigmoid_wVecot, sigmoid_b, softmax_wVecot, softmax_b) =
      (weight.sigmoid_wVecot, weight.sigmoid_b, weight.softmax_wVecot, weight.softmax_b)

    dataBlock.resetReadIndex()
    for (_ <- 0 until dataBlock.size) {
      val data = dataBlock.read()
      val x = data.getX
      val y = data.getY

      val softmax = (0 until rank).map(i => softmax_wVecot(i).dot(x) + softmax_b(i)).toArray
      Maths.softmax(softmax)
      val sigmoid = (0 until rank).map(i => Maths.sigmoid({
        var temp = sigmoid_wVecot(i).dot(x) + sigmoid_b(i)
        temp = math.max(temp, -18)
        temp = math.min(temp, 18)
        temp
      })).toArray
      val pre = (0 until rank).map(i => softmax(i) * sigmoid(i)).sum

      loss += {
        if (y == 1) -Math.log(pre)
        else -Math.log(1 - pre)
      }
    }
    loss /= dataBlock.size()
    loss += {
      (0 until rank).map(i => {
        sigmoid_wVecot(i).dot(sigmoid_wVecot(i)) + softmax_wVecot(i).dot(softmax_wVecot(i))
      }).sum * 0.5 * reg
    }
    loss
  }

  def initModels(): Unit = {
    val totalTask = ctx.getTotalTaskNum
    val taskId = ctx.getTaskId.getIndex
    val random = new Random()

    for (row <- 0 until rank) {
      if (row % totalTask == taskId) {
        val randV = new DenseDoubleVector(indexRange.toInt)
        randV.setRowId(row.toInt)

        for (col <- 0 until indexRange.toInt) {
          randV.set(col, vInit * random.nextGaussian())
        }

        mlrModel.sigmoid_weight.increment(randV)
      }
    }
    mlrModel.sigmoid_weight.syncClock()
  }

}

