/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.core.graphsubmit

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.mlcore.network.Graph
import com.tencent.angel.mlcore.network.layers.unary.KmeansInputLayer
import com.tencent.angel.mlcore.optimizer.decayer.StepSizeScheduler
import com.tencent.angel.mlcore.utils.ValidationUtils
import com.tencent.angel.mlcore.MLModel
import com.tencent.angel.ml.core.{AngelMasterContext, MLLearner}
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

class GraphLearner(conf: SharedConf, ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[GraphLearner])
  private implicit val sharedConf: SharedConf = conf

  val epochNum: Int = conf.epochNum
  val indexRange: Long = conf.indexRange
  val modelSize: Long = conf.modelSize
  val lr0: Double = conf.learningRate

  // Init Graph Model
  val modelClassName: String = sharedConf.modelClassName
  val model: AngelModel = AngelModel(modelClassName, conf, ctx)
  model.buildNetwork()
  model.createMatrices(AngelMasterContext(null))
  val graph: Graph = model.graph
  val ssScheduler: StepSizeScheduler = StepSizeScheduler(conf.stepSizeScheduler, lr0)
  val decayOnBatch: Boolean = conf.getBoolean(MLCoreConf.ML_OPT_DECAY_ON_BATCH,
    MLCoreConf.DEFAULT_ML_OPT_DECAY_ON_BATCH)

  def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
    var batchCount: Int = 0
    var loss: Double = 0.0
    while (iter.hasNext) {
      // LOG.info("start to feedData ...")
      graph.feedData(iter.next())

      // LOG.info("start to pullParams ...")
      if (model.isSparseFormat) {
        model.pullParams(epoch, graph.placeHolder.getIndices)
      } else {
        model.pullParams(epoch)
      }


      // LOG.info("calculate to forward ...")
      loss = graph.calForward() // forward
      // LOG.info(s"The training los of epoch $epoch batch $batchCount is $loss" )

      // LOG.info("calculate to backward ...")
      graph.calBackward() // backward

      // LOG.info("calculate and push gradient ...")
      model.pushGradient(graph.getLR) // pushgrad
      // waiting all gradient pushed

      // LOG.info("waiting for push barrier ...")
      barrier()

      if (decayOnBatch) {
        graph.setLR(ssScheduler.next())
      }
      if (ctx.getTaskId.getIndex == 0) {
        // LOG.info("start to update ...")
        model.update(epoch * numBatch + batchCount, 1) // update parameters on PS
      }

      // waiting all gradient update finished
      // LOG.info("waiting for update barrier ...")
      barrier()
      batchCount += 1

      LOG.info(s"epoch $epoch batch $batchCount is finished!")
    }

    loss
  }

  /**
    * train LR model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    train(trainData, null, validationData)
  }

  override def train(posTrainData: DataBlock[LabeledData],
            negTrainData: DataBlock[LabeledData],
            validationData: DataBlock[LabeledData]): MLModel = {
    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train ...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr0")

    val trainDataSize = if (negTrainData == null) posTrainData.size() else {
      posTrainData.size() + negTrainData.size()
    }

    globalMetrics.addMetric(AngelMLConf.TRAIN_LOSS, LossMetric(trainDataSize))
    globalMetrics.addMetric(AngelMLConf.VALID_LOSS, LossMetric(validationData.size))

    val loadModelPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")
    if (loadModelPath.isEmpty) {
      model.init(AngelMasterContext(null))
    }

    barrier()

    val numBatch = conf.numUpdatePerEpoch
    val batchSize: Int = (trainDataSize + numBatch - 1) / numBatch
    val batchData = new Array[LabeledData](batchSize)

    if (conf.useShuffle) {
      posTrainData.shuffle()
      if (negTrainData != null) {
        negTrainData.shuffle()
      }
    }

    // Init cluster centers randomly
    graph.getInputLayer("input") match {
      case layer: KmeansInputLayer if ctx.getTaskId.getIndex == 0 && conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "").isEmpty =>
        model.pullParams(0)
        val K = conf.numClass
        layer.initKCentersRandomly(ctx.getTotalTaskNum, posTrainData, K)
        model.pushGradient(graph.getLR)
      case _ =>
    }

    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val iter = if (negTrainData == null) {
        getBathDataIterator(posTrainData, batchData, numBatch)
      } else {
        getBathDataIterator(posTrainData, negTrainData, batchData, numBatch)
      }

      val startTrain = System.currentTimeMillis()
      if (!decayOnBatch) {
        graph.setLR(ssScheduler.next())
      }
      val loss: Double = trainOneEpoch(epoch, iter, numBatch)
      val trainCost = System.currentTimeMillis() - startTrain
      globalMetrics.metric(AngelMLConf.TRAIN_LOSS, loss * trainDataSize)
      LOG.info(s"$epoch-th training finished! the trainCost is $trainCost")

      val startValid = System.currentTimeMillis()
      if (validationData != null && validationData.size() > 0) {
        LOG.info(s"Begin to validate in $epoch-th epoch")
        validate(epoch, validationData)
      }
      val validCost = System.currentTimeMillis() - startValid


      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch success. " +
        s"epoch cost ${trainCost + validCost} ms." +
        s"train cost $trainCost ms. " +
        s"validation cost $validCost ms.")

      ctx.incEpoch()
    }

    model.graph.timeStats.summary()
    model
  }

  private def getBathDataIterator(trainData: DataBlock[LabeledData],
                                  batchData: Array[LabeledData], numBatch: Int) = {
    trainData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        batchData.indices.foreach { i => batchData(i) = trainData.loopingRead() }
        count += 1
        batchData
      }
    }
  }

  private def getBathDataIterator(posData: DataBlock[LabeledData],
                                  negData: DataBlock[LabeledData],
                                  batchData: Array[LabeledData], numBatch: Int) = {
    posData.resetReadIndex()
    negData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0
      val posnegRatio: Double = conf.posnegRatio()
      val posPreNum: Int = Math.max((posData.size() + numBatch - 1) / numBatch,
        batchData.length * posnegRatio / (1.0 + posnegRatio)).toInt

      val posNum: Int = if (posPreNum < 0.5 * batchData.length) {
        Math.max(1, posPreNum)
      } else {
        batchData.length / 2
      }
      val negNum: Int = batchData.length - posNum

      LOG.info(s"The exact pos/neg is ${1.0 * posNum / negNum} ")

      val posDropRate: Double = if (posNum * numBatch > posData.size()) {
        0.0
      } else {
        1.0 * (posData.size() - posNum * numBatch) / posData.size()
      }

      LOG.info(s"${posDropRate * 100}% of positive data will be discard in task ${ctx.getTaskIndex}")

      val negDropRate: Double = if (negNum * numBatch > negData.size()) {
        0.0
      } else {
        1.0 * (negData.size() - negNum * numBatch) / negData.size()
      }

      LOG.info(s"${negDropRate * 100}% of negative data will be discard in task ${ctx.getTaskIndex}")

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        (0 until posNum).foreach { i =>
          if (posDropRate == 0) {
            batchData(i) = posData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val pos = posData.loopingRead()
              if (Math.random() > posDropRate) {
                batchData(i) = pos
                flag = false
              }
            }
          }
        }

        (0 until negNum).foreach { i =>
          if (negDropRate == 0) {
            batchData(i + posNum) = negData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val neg = negData.loopingRead()
              if (Math.random() > negDropRate) {
                batchData(i + posNum) = neg
                flag = false
              }
            }
          }
        }

        count += 1
        batchData
      }
    }
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit = {
    val predList = model.predict(valiData)
    ValidationUtils.calMetrics(epoch, predList, graph.getLossFunc)

    // Note: the data has feed in `model.predict(valiData)`
    globalMetrics.metric(AngelMLConf.VALID_LOSS,
      graph.getLossLayer.calLoss * valiData.size())
  }
}
