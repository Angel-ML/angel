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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.GBDT

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.MathUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{TDoubleVector, TIntVector}
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.ml.algorithm.GBDT.GBDTModel._
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.psagent.matrix.MatrixClient
import com.tencent.angel.worker.predict.PredictResult

object GBDTModel {
  val SKETCH_MAT: String = "gbdt.sketch"
  val GRAD_HIST_MAT_PREFIX: String = "gbdt.grad.histogram.node"
  val ACTIVE_NODE_MAT: String = "gbdt.active.nodes"
  val FEAT_SAMPLE_MAT: String = "gbdt.feature.sample."
  val SPLIT_FEAT_MAT: String = "gbdt.split.feature"
  val SPLIT_VALUE_MAT: String = "gbdt.split.value"
  val SPLIT_GAIN_MAT: String = "gbdt.split.gain"
  val NODE_GRAD_MAT: String = "gbdt.node.grad.stats"
  val NODE_PRED_MAT: String = "gbdt.node.predict"

  val METRIC_MAT: String = "gbdt.metric"
}

class GBDTModel(conf: Configuration) extends AlgorithmModel {

  var LOG = LogFactory.getLog(classOf[GBDTModel])

  val featNum = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  val featNonzero = conf.getInt(MLConf.ML_FEATURE_NNZ, MLConf.DEFAULT_ML_FEATURE_NNZ)
  val maxTreeNum = conf.getInt(MLConf.ML_GBDT_TREE_NUM, MLConf.DEFAULT_ML_GBDT_TREE_NUM)
  val maxTreeDepth = conf.getInt(MLConf.ML_GBDT_TREE_DEPTH, MLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
  val splitNum = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM, MLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
  val featSampleRatio = conf.getFloat(MLConf.ML_GBDT_SAMPLE_RATIO, MLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)

  val maxTNodeNum: Int = MathUtils.pow(2, maxTreeDepth) - 1
  val sampFeatNum: Int = (featNum * featSampleRatio).toInt

  // # parameter server
  val psNumber = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, 1)

  // Matrix 1: quantile sketch
  val sketch = new PSModel[TDoubleVector](SKETCH_MAT,
    1, featNum * splitNum, 1, featNum * splitNum / psNumber)
  sketch.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  sketch.setOplogType("DENSE_DOUBLE")
  addPSModel(SKETCH_MAT, sketch)

  // Matrix 2: sampled feature
  val featSample = new PSModel[TIntVector](FEAT_SAMPLE_MAT,
    maxTreeNum, sampFeatNum, 1, sampFeatNum / psNumber)
  featSample.setRowType(MLProtos.RowType.T_INT_DENSE)
  featSample.setOplogType("DENSE_INT")
  addPSModel(FEAT_SAMPLE_MAT, featSample)

  val histMats: Array[PSModel[TDoubleVector]] = new Array[PSModel[TDoubleVector]](maxTNodeNum)
  // Matrix 3: gradient and hess histogram, one for each node
  for (nid <- 0 until maxTNodeNum) {
    val histMat = new PSModel[TDoubleVector](GRAD_HIST_MAT_PREFIX + nid,
      1, 2 * this.splitNum * sampFeatNum, 1, sampFeatNum / psNumber)
    histMat.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
    histMat.setOplogType("DENSE_DOUBLE")
    addPSModel(GRAD_HIST_MAT_PREFIX + nid, histMat)
    histMats(nid) = histMat
  }

  // Matrix 4: active tree nodes
  val activeTNodes = new PSModel[TIntVector](ACTIVE_NODE_MAT,
    1, maxTNodeNum, 1, maxTNodeNum / psNumber)
  activeTNodes.setRowType(MLProtos.RowType.T_INT_DENSE)
  activeTNodes.setOplogType("DENSE_INT")
  addPSModel(ACTIVE_NODE_MAT, activeTNodes)

  // Matrix 5: split feature
  val splitFeat = new PSModel[TIntVector](SPLIT_FEAT_MAT,
    maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitFeat.setRowType(MLProtos.RowType.T_INT_DENSE)
  splitFeat.setOplogType("DENSE_INT")
  addPSModel(SPLIT_FEAT_MAT, splitFeat)

  // Matrix 6: split value
  val splitValue = new PSModel[TDoubleVector](SPLIT_VALUE_MAT,
    maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitValue.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  splitValue.setOplogType("DENSE_DOUBLE")
  addPSModel(SPLIT_VALUE_MAT, splitValue)

  // Matrix 7: split loss gain
  val splitGain = new PSModel[TDoubleVector](SPLIT_GAIN_MAT,
    maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitGain.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  splitGain.setOplogType("DENSE_DOUBLE")
  addPSModel(SPLIT_GAIN_MAT, splitGain)

  // Matrix 8: node's grad stats
  val nodeGradStats = new PSModel[TDoubleVector](NODE_GRAD_MAT,
    maxTreeNum, 2 * maxTNodeNum, 1, 2 * maxTNodeNum / psNumber)
  nodeGradStats.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  nodeGradStats.setOplogType("DENSE_DOUBLE")
  addPSModel(NODE_GRAD_MAT, nodeGradStats)

  // Matrix 9: node's predict value
  val nodePred = new PSModel[TDoubleVector](NODE_PRED_MAT,
    maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  nodePred.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  nodePred.setOplogType("DENSE_DOUBLE")
  addPSModel(NODE_PRED_MAT, nodePred)

  var _localSketch: MatrixClient = null
  var _localFeatSample: MatrixClient = null
  var _localHistMats: Array[MatrixClient] = new Array[MatrixClient](maxTNodeNum)
  var _localActiveTNodes: MatrixClient = null
  var _localSplitFeat: MatrixClient = null
  var _localSplitValue: MatrixClient = null
  var _localSplitGain: MatrixClient = null
  var _localNodeGradStats: MatrixClient = null
  var _localNodePred: MatrixClient = null

  def init(ctx: TaskContext) = {
    _localSketch = ctx.getMatrix(sketch.getName)
    _localFeatSample = ctx.getMatrix(featSample.getName)
    for (nid <- 0 until maxTNodeNum) {
      _localHistMats(nid) = ctx.getMatrix(GRAD_HIST_MAT_PREFIX + nid)
    }
    _localActiveTNodes = ctx.getMatrix(activeTNodes.getName)
    _localSplitFeat = ctx.getMatrix(splitFeat.getName)
    _localSplitValue = ctx.getMatrix(splitValue.getName)
    _localSplitGain = ctx.getMatrix(splitGain.getName)
    _localNodeGradStats = ctx.getMatrix(nodeGradStats.getName)
    _localNodePred = ctx.getMatrix(nodePred.getName)
  }

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    sketch.setSavePath(path + "/" + sketch.getName)
    featSample.setSavePath(path + "/" + featSample.getName)
    for (nid <- 0 until maxTNodeNum) {
      histMats(nid).setSavePath(path + "/" + GRAD_HIST_MAT_PREFIX + nid)
    }
    activeTNodes.setSavePath(path + "/" + activeTNodes.getName)
    splitFeat.setSavePath(path + "/" + splitFeat.getName)
    splitValue.setSavePath(path + "/" + splitValue.getName)
    splitGain.setSavePath(path + "/" + splitGain.getName)
    nodeGradStats.setSavePath(path + "/" + nodeGradStats.getName)
    nodePred.setSavePath(path + "/" + nodePred.getName)
  }

  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)

    sketch.setLoadPath(path + "/" + sketch.getName)
    featSample.setLoadPath(path + "/" + featSample.getName)
    for (nid <- 0 until maxTNodeNum) {
      histMats(nid).setLoadPath(path + "/" + GRAD_HIST_MAT_PREFIX + nid)
    }
    activeTNodes.setLoadPath(path + "/" + activeTNodes.getName)
    splitFeat.setLoadPath(path + "/" + splitFeat.getName)
    splitValue.setLoadPath(path + "/" + splitValue.getName)
    splitGain.setLoadPath(path + "/" + splitGain.getName)
    nodeGradStats.setLoadPath(path + "/" + nodeGradStats.getName)
    nodePred.setLoadPath(path + "/" + nodePred.getName)
  }

  override def predict(dataSet: Storage[LabeledData]): Storage[PredictResult] = {
    val predict = new MemoryStorage[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.getTotalElemNum) {
      val instance = dataSet.read
      val id = instance.getY

    }
    predict
  }
}
