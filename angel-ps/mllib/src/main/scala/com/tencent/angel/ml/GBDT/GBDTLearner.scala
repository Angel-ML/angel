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


package com.tencent.angel.ml.GBDT

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.GBDT.algo.RegTree.RegTDataStore
import com.tencent.angel.ml.GBDT.algo.{GBDTController, GBDTPhase}
import com.tencent.angel.ml.GBDT.param.{GBDTParam, RegTParam}
import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.ml.metric.ErrorMetric
import com.tencent.angel.ml.model.OldMLModel
import com.tencent.angel.mlcore.optimizer.decayer.StepSizeScheduler
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

class GBDTLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[GBDTLearner])

  val param = new GBDTParam

  val model = new GBDTModel(conf, ctx)

  def initParam(): Unit = {

    // 1. set training param
    param.taskType = conf.get(AngelMLConf.ML_GBDT_TASK_TYPE, AngelMLConf.DEFAULT_ML_GBDT_TASK_TYPE)
    param.numFeature = conf.getInt(AngelMLConf.ML_FEATURE_INDEX_RANGE, AngelMLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    param.numNonzero = conf.getInt(AngelMLConf.ML_MODEL_SIZE, AngelMLConf.DEFAULT_ML_MODEL_SIZE)
    param.numSplit = conf.getInt(AngelMLConf.ML_GBDT_SPLIT_NUM, AngelMLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
    param.treeNum = conf.getInt(AngelMLConf.ML_GBDT_TREE_NUM, AngelMLConf.DEFAULT_ML_GBDT_TREE_NUM)
    param.maxDepth = conf.getInt(AngelMLConf.ML_GBDT_TREE_DEPTH, AngelMLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
    param.colSample = conf.getFloat(AngelMLConf.ML_GBDT_SAMPLE_RATIO, AngelMLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)
    param.learningRate = conf.getFloat(AngelMLConf.ML_LEARN_RATE, AngelMLConf.DEFAULT_ML_LEARN_RATE.asInstanceOf[Float])
    param.maxThreadNum = conf.getInt(AngelMLConf.ML_GBDT_THREAD_NUM, AngelMLConf.DEFAULT_ML_GBDT_THREAD_NUM)
    param.batchSize = conf.getInt(AngelMLConf.ML_GBDT_BATCH_SIZE, AngelMLConf.DEFAULT_ML_GBDT_BATCH_SIZE)
    param.isServerSplit = conf.getBoolean(AngelMLConf.ML_GBDT_SERVER_SPLIT, AngelMLConf.DEFAULT_ML_GBDT_SERVER_SPLIT)

    // 2. set parameter name on PS
    param.sketchName = GBDTModel.SKETCH_MAT
    param.gradHistNamePrefix = GBDTModel.GRAD_HIST_MAT_PREFIX
    param.activeTreeNodesName = GBDTModel.ACTIVE_NODE_MAT
    param.sampledFeaturesName = GBDTModel.FEAT_SAMPLE_MAT
    param.cateFeatureName = GBDTModel.FEAT_CATEGORY_MAT
    param.splitFeaturesName = GBDTModel.SPLIT_FEAT_MAT
    param.splitValuesName = GBDTModel.SPLIT_VALUE_MAT
    param.splitGainsName = GBDTModel.SPLIT_GAIN_MAT
    param.nodeGradStatsName = GBDTModel.NODE_GRAD_MAT
    param.nodePredsName = GBDTModel.NODE_PRED_MAT

  }

  def initDataMeta(dataSet: DataBlock[LabeledData], param: RegTParam): RegTDataStore = {

    val numFeature: Int = param.numFeature
    val numNonzero: Int = param.numNonzero
    LOG.info(s"Create data meta, numFeature=$numFeature, nonzero=$numNonzero")

    val dataStore: RegTDataStore = new RegTDataStore(param)

    dataStore.init(dataSet)

    LOG.info(s"Finish creating data meta, numRow=${dataStore.numRow}, " +
      s"numCol=${dataStore.numCol}, nonzero=${dataStore.numNonzero}")

    dataStore
  }

  def updateMetrics(controller: GBDTController): Unit = {
    val trainMetrics = controller.eval
    val validMetrics = controller.predict
    globalMetrics.metric(AngelMLConf.TRAIN_ERROR, trainMetrics._1)
    globalMetrics.metric(AngelMLConf.VALID_ERROR, validMetrics._1)
  }

  /**
    * train GBDT model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def trainOld(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): OldMLModel = {
    LOG.debug("------GBDT starts training------")

    LOG.info("1. initialize")

    initParam

    val dataGenStartTs: Long = System.currentTimeMillis

    val trainDataStore: RegTDataStore = initDataMeta(trainData, param)
    val validDataStore: RegTDataStore = initDataMeta(validationData, param)

    LOG.info(s"Build data info cost ${System.currentTimeMillis - dataGenStartTs} ms")

    LOG.info("2.train")
    val trainStartTs: Long = System.currentTimeMillis

    val controller: GBDTController = new GBDTController(ctx, param,
      trainDataStore, validDataStore, model)
    controller.init

    globalMetrics.addMetric(AngelMLConf.TRAIN_ERROR, ErrorMetric(trainDataStore.numRow))
    globalMetrics.addMetric(AngelMLConf.VALID_ERROR, ErrorMetric(validDataStore.numRow))

    while (controller.phase != GBDTPhase.FINISHED) {
      LOG.info(s"******Current phase: ${controller.phase}, clock[${controller.clock}]******")

      controller.phase match {
        case GBDTPhase.CREATE_SKETCH => controller.createSketch
        case GBDTPhase.GET_SKETCH => controller.getSketch
        case GBDTPhase.SAMPLE_FEATURE => controller.sampleFeature
        case GBDTPhase.NEW_TREE => controller.createNewTree
        case GBDTPhase.RUN_ACTIVE => controller.runActiveNode
        case GBDTPhase.FIND_SPLIT => controller.findSplit
        case GBDTPhase.AFTER_SPLIT => controller.afterSplit
        case GBDTPhase.FINISH_TREE => {
          controller.finishCurrentTree
          updateMetrics(controller)
        }
        case _ => throw new AngelException("Unrecognizable GBDT phase: " + controller.phase)
      }

      controller.updatePhase
      controller.incrementClock
      ctx.incEpoch
    }

    LOG.info(s"Task[${ctx.getTaskIndex}] finishes training, " +
      s"train phase cost ${System.currentTimeMillis - trainStartTs} ms, " +
      s"total clock ${controller.clock}")

    model
  }

  override protected val ssScheduler: StepSizeScheduler = null

  override protected def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = ???

  override protected def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit = ???
}