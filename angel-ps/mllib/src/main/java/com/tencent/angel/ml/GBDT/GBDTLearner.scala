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

package com.tencent.angel.ml.GBDT

import java.util.{ArrayList, List}

import com.tencent.angel.ml.GBDT.algo.RegTree.RegTDataStore
import com.tencent.angel.ml.GBDT.algo.{FeatureMeta, GBDTController, GBDTPhase}
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector
import com.tencent.angel.ml.metric.ErrorMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.param.{GBDTParam, RegTParam}
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

class GBDTLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG = LogFactory.getLog(classOf[GBDTLearner])

  val param = new GBDTParam

  // 1. set training param
  param.taskType = conf.get(MLConf.ML_GBDT_TASK_TYPE, MLConf.DEFAULT_ML_GBDT_TASK_TYPE)
  param.numFeature = conf.getInt(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  param.numNonzero = conf.getInt(MLConf.ML_MODEL_SIZE, MLConf.DEFAULT_ML_MODEL_SIZE)
  param.numSplit = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM, MLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
  param.treeNum = conf.getInt(MLConf.ML_GBDT_TREE_NUM, MLConf.DEFAULT_ML_GBDT_TREE_NUM)
  param.maxDepth = conf.getInt(MLConf.ML_GBDT_TREE_DEPTH, MLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
  param.colSample = conf.getFloat(MLConf.ML_GBDT_SAMPLE_RATIO, MLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)
  param.learningRate = conf.getFloat(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE.asInstanceOf[Float])
  param.maxThreadNum = conf.getInt(MLConf.ML_GBDT_THREAD_NUM, MLConf.DEFAULT_ML_GBDT_THREAD_NUM)
  param.batchNum = conf.getInt(MLConf.ML_GBDT_BATCH_NUM, MLConf.DEFAULT_ML_GBDT_BATCH_NUM)
  param.isServerSplit = conf.getBoolean(MLConf.ML_GBDT_SERVER_SPLIT, MLConf.DEFAULT_ML_GBDT_SERVER_SPLIT)

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

  // 3. create and init GBDT model
  val model = new GBDTModel(conf, ctx)


  def initDataMetaInfo(trainDataStorage: DataBlock[LabeledData], param: RegTParam): RegTDataStore = {
    var totalSample: Int = 0
    val numFeature: Int = param.numFeature
    val numNonzero: Int = param.numNonzero
    LOG.info(s"Create data meta, numFeature=$numFeature, nonzero=$numNonzero")

    val dataStore: RegTDataStore = new RegTDataStore(param)

    val instances: List[SparseDoubleSortedVector] = new ArrayList[SparseDoubleSortedVector]
    val labels: List[java.lang.Float] = new ArrayList[java.lang.Float]
    val preds: List[java.lang.Float] = new ArrayList[java.lang.Float]
    // max and min of each feature
    val minFeatures: List[java.lang.Float] = new ArrayList[java.lang.Float]
    val maxFeatures: List[java.lang.Float] = new ArrayList[java.lang.Float]

    for (i <- 0 to numFeature - 1) {
      minFeatures.add(0.0f)
      maxFeatures.add(Float.MinValue)
    }

    val weights: List[java.lang.Float] = new ArrayList[java.lang.Float]

    trainDataStorage.resetReadIndex
    var data: LabeledData = trainDataStorage.read
    var isFinish = false
    if (data == null) {
      isFinish = true
    }

    while (!isFinish) {
      val x: SparseDoubleSortedVector = data.getX.asInstanceOf[SparseDoubleSortedVector]
      var y: Float = data.getY.toFloat
      if (y != 1) {
        y = 0
      }
      val indices: Array[Int] = x.getIndices
      val values: Array[Double] = x.getValues
      for (i <- 0 to (indices.length - 1)) {
        val fid: Int = indices(i)
        if (values(i) > maxFeatures.get(fid)) {
          maxFeatures.set(fid, values(i).toFloat)
        }
        if (values(i) < minFeatures.get(fid)) {
          minFeatures.set(fid, values(i).toFloat)
        }
      }
      instances.add(x)
      labels.add(y)
      preds.add(0.0f)
      weights.add(1.0f)
      totalSample += 1
      data = trainDataStorage.read
      if (data == null) {
        isFinish = true
      }
    }

    val featureMeta: FeatureMeta = new FeatureMeta(numFeature,
      Maths.floatList2Arr(minFeatures), Maths.floatList2Arr(maxFeatures))
    dataStore.setNumRow(totalSample)
    dataStore.setNumCol(numFeature)
    dataStore.setNumNonzero(numNonzero)
    dataStore.setInstances(instances)
    dataStore.setLabels(Maths.floatList2Arr(labels))
    dataStore.setPreds(Maths.floatList2Arr(preds))
    dataStore.setFeatureMeta(featureMeta)
    dataStore.setWeights(Maths.floatList2Arr(weights))
    dataStore.setBaseWeights(Maths.floatList2Arr(weights))

    LOG.info(s"Finish creating data meta, numRow=$totalSample, numCol=$numFeature, nonzero=$numNonzero")

    dataStore
  }

  /**
    * train GBDT model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override
  def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    LOG.debug("------GBDT starts training------")

    LOG.info("1. initialize")
    val dataGenStartTs: Long = System.currentTimeMillis

    val trainDataStore: RegTDataStore = initDataMetaInfo(trainData, param)
    val validDataStore: RegTDataStore = initDataMetaInfo(validationData, param)

    LOG.info(s"Build data info cost ${System.currentTimeMillis - dataGenStartTs} ms")
    assert(trainDataStore.numRow == trainDataStore.instances.size)
    assert(validDataStore.numRow == validDataStore.instances.size)

    LOG.info("2.train")
    val trainStartTs: Long = System.currentTimeMillis

    val controller: GBDTController = new GBDTController(ctx, param, trainDataStore, validDataStore, model)
    controller.init

    globalMetrics.addMetric(MLConf.TRAIN_ERROR, ErrorMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_ERROR, ErrorMetric(validationData.size))

    while (controller.phase != GBDTPhase.FINISHED) {

      var nextClock = false
      if (!nextClock && controller.phase == GBDTPhase.CREATE_SKETCH) {
        LOG.info(s"******Current phase: CREATE_SKETCH, clock[${controller.clock}]******")
        controller.createSketch
        controller.mergeCateFeatSketch
        controller.setPhase(GBDTPhase.GET_SKETCH)
        nextClock = true
      }
      else if (!nextClock && controller.phase == GBDTPhase.GET_SKETCH) {
        LOG.info(s"******Current phase: GET_SKETCH, clock[${controller.clock}]******")
        controller.getSketch
        controller.sampleFeature
        controller.setPhase(GBDTPhase.NEW_TREE)
        nextClock = true
      }
      else if (!nextClock && controller.phase == GBDTPhase.NEW_TREE) {
        LOG.info(s"******Current phase: NEW_TREE, clock[${controller.clock}]******")
        controller.createNewTree
        controller.runActiveNode
        controller.setPhase(GBDTPhase.FIND_SPLIT)
        nextClock = true
      }
      else if (!nextClock && controller.phase == GBDTPhase.RUN_ACTIVE) {
        LOG.info(s"******Current phase: RUN_ACTIVE, clock[${controller.clock}]******")
        controller.runActiveNode
        controller.setPhase(GBDTPhase.FIND_SPLIT)
        nextClock = true
      }
      else if (!nextClock && controller.phase == GBDTPhase.FIND_SPLIT) {
        LOG.info(s"******Current phase: FIND_SPLIT, clock[${controller.clock}]******")
        controller.findSplit
        controller.setPhase(GBDTPhase.AFTER_SPLIT)
        nextClock = true
      }
      else if (!nextClock && controller.phase == GBDTPhase.AFTER_SPLIT) {
        LOG.info(s"******Current phase: AFTER_SPLIT, clock[${controller.clock}]******")
        controller.afterSplit
        val hasActive: Boolean = controller.hasActiveTNode
        if (hasActive) {
          controller.finishCurrentDepth
          controller.setPhase(GBDTPhase.RUN_ACTIVE)
        }
        else {
          controller.updateInsPreds
          controller.updateLeafPreds
          val trainMetrics = controller.eval
          val validMetrics = controller.predict
          globalMetrics.metric(MLConf.TRAIN_ERROR, trainMetrics._1)
          globalMetrics.metric(MLConf.VALID_ERROR, validMetrics._1)
          controller.finishCurrentTree()
          controller.setPhase(GBDTPhase.NEW_TREE)
          controller.sampleFeature
          if (controller.isFinished) {
            controller.setPhase(GBDTPhase.FINISHED)
          }
        }
        nextClock = true
      }
      controller.clock += 1
      ctx.incEpoch
    }

    LOG.info(s"Task[${ctx.getTaskIndex}] finishes training, " +
      s"train phase cost ${System.currentTimeMillis - trainStartTs} ms, " +
      s"total clock ${controller.clock}")

    model
  }

}

