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

package com.tencent.angel.ml.treemodels.gbdt

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.treemodels.sketch.{HeapQuantileSketch, SketchUtils}
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration


object GBDTModel {
  val SYNC = "gbdt.sync"
  val INSTANCE_NUM_MAT = "gbdt.instance.num"
  val NNZ_NUM_MAT = "gbdt.nnz.num"
  val SKETCH_MAT = "gbdt.sketch"
  val SPLIT_FEAT_MAT = "gbdt.split.feature"
  val SPLIT_VALUE_MAT = "gbdt.split.value"
  val SPLIT_GAIN_MAT = "gbdt.split.gain"
  val NODE_GRAD_MAT = "gbdt.node.grad"
  val NODE_PRED_MAT = "gbdt.node.pred"

  // data parallel matrices
  val FEAT_SAMPLE_MAT = "gbdt.feature.subsample"
  val GRAD_HIST_MAT_PREFIX = "gbdt.grad.histogram.node"

  // feature parallel matrices
  val INS_SAMPLE_MAT = "gbdt.instance.subsample"
  val FEAT_ROW_MAT = "gbdt.feature.row"
  val LABEL_MAT = "gbdt.label"
  val LOCAL_SPLIT_FEAT_MAT = "gbdt.local.split.feature"
  val LOCAL_SPLIT_VALUE_MAT = "gbdt.local.split.value"
  val LOCAL_SPLIT_GAIN_MAT = "gbdt.local.split.gain"
  val SPLIT_RESULT_MAT = "gbdt.split.result"


  private val LOG: Log = LogFactory.getLog(classOf[GBDTModel])
}

abstract class GBDTModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG: Log = GBDTModel.LOG

  protected val numFeature = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  protected val numSplit = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM, MLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
  protected val maxDepth = conf.getInt(MLConf.ML_GBDT_TREE_DEPTH, MLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
  protected val maxNodeNum = conf.getInt(MLConf.ML_GBDT_MAX_NODE_NUM, Maths.pow(2, maxDepth) - 1)
  protected val featSampleRatio = conf.getFloat(MLConf.ML_GBDT_SAMPLE_RATIO, MLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)
  protected val numTree = conf.getInt(MLConf.ML_GBDT_TREE_NUM, MLConf.DEFAULT_ML_GBDT_TREE_NUM)
  protected val numClass = conf.getInt(MLConf.ML_GBDT_CLASS_NUM, MLConf.DEFAULT_ML_GBDT_CLASS_NUM)

  protected val numWorker = conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER)
  protected val numPS = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER)
  protected var batchSize: Int = 1024
  if (batchSize == -1 || batchSize > numFeature) {
    batchSize = numFeature
  }

  protected val numInstance = conf.getInt("angel.ml.train.data.num", 100)
  protected val numFeatNnz = conf.getInt("angel.ml.max.feat.nnz", 100)

  LOG.info(s"numFeature[$numFeature], numInstance[$numInstance], numFeatNnz[$numFeatNnz]")

  // Matrix 0: synchronization
  private val syncMat = PSModel(GBDTModel.SYNC, 1, 1)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)
  addPSModel(GBDTModel.SYNC, syncMat)

  // Matrix 1: number of instance on each worker
  private val insNumMat = PSModel(GBDTModel.INSTANCE_NUM_MAT, 1, numWorker)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setHogwild(true)
    .setNeedSave(false)
  addPSModel(GBDTModel.INSTANCE_NUM_MAT, insNumMat)

  // Matrix 2: nnz of each feature
  private val nnzMat = PSModel(GBDTModel.NNZ_NUM_MAT, numWorker, numFeature, 1, numFeature)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)
  addPSModel(GBDTModel.NNZ_NUM_MAT, nnzMat)

  // Matrix 3: global quantile sketches
  // TODO: create at runtime (after max nnz of feature is known)
  private val maxQSketchSize = 1 + SketchUtils.needBufferCapacity(HeapQuantileSketch.DEFAULT_K, numFeatNnz.toLong)
  private val sketchMat = PSModel(GBDTModel.SKETCH_MAT, batchSize, maxQSketchSize, batchSize / numPS, maxQSketchSize)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
    .setNeedSave(false)
  addPSModel(GBDTModel.SKETCH_MAT, sketchMat)

  // Matrix 4: split feature id
  private val splitFeat = new PSModel(GBDTModel.SPLIT_FEAT_MAT, numTree, maxNodeNum, numTree / numPS, maxNodeNum)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
  addPSModel(GBDTModel.SPLIT_FEAT_MAT, splitFeat)

  // Matrix 5: split value
  private val splitValue = PSModel(GBDTModel.SPLIT_VALUE_MAT, numTree, maxNodeNum, numTree / numPS, maxNodeNum)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(GBDTModel.SPLIT_VALUE_MAT, splitValue)

  // Matrix 5: split loss gain
  private val splitGain = PSModel(GBDTModel.SPLIT_GAIN_MAT, numTree, maxNodeNum, numTree / numPS, maxNodeNum)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
    .setNeedSave(false)
  addPSModel(GBDTModel.SPLIT_GAIN_MAT, splitGain)

  // Matrix 7: node's grad stats
  private val nodeGradStats = PSModel(GBDTModel.NODE_GRAD_MAT, numTree, 2 * maxNodeNum, numTree / numPS, 2 * maxNodeNum)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
    .setNeedSave(false)
  addPSModel(GBDTModel.NODE_GRAD_MAT, nodeGradStats)

  // Matrix 8: node's predict value
  private val nodePred = PSModel(GBDTModel.NODE_PRED_MAT, numTree, maxNodeNum, numTree / numPS, maxNodeNum)
    .setRowType(RowType.T_FLOAT_DENSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(GBDTModel.NODE_PRED_MAT, nodePred)


  def sync(): Unit = {
    val syncModel = getPSModel(GBDTModel.SYNC)
    syncModel.clock().get()
    syncModel.getRow(0)
    LOG.debug("******SYNCHRONIZATION******")
  }

  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???
}
