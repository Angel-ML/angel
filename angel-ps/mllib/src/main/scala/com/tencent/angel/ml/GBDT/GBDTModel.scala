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

import java.text.DecimalFormat

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.GBDT.GBDTModel._
import com.tencent.angel.ml.GBDT.algo.sketch.{HeapQuantileSketch, SketchUtils}
import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntIntVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.core.utils.Maths
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object GBDTModel {

  val SKETCH_MAT: String = "gbdt.sketch"
  val GRAD_HIST_MAT_PREFIX: String = "gbdt.grad.histogram.node"
  val ACTIVE_NODE_MAT: String = "gbdt.active.nodes"
  val FEAT_SAMPLE_MAT: String = "gbdt.feature.sample"
  val FEAT_CATEGORY_MAT = "gbdt.feature.category"
  val SPLIT_FEAT_MAT: String = "gbdt.split.feature"
  val SPLIT_VALUE_MAT: String = "gbdt.split.value"
  val SPLIT_GAIN_MAT: String = "gbdt.split.gain"
  val NODE_GRAD_MAT: String = "gbdt.node.grad.stats"
  val NODE_PRED_MAT: String = "gbdt.node.predict"


  def apply(conf: Configuration) = {
    new GBDTModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new GBDTModel(conf, ctx)
  }
}

class GBDTModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  var LOG = LogFactory.getLog(classOf[GBDTModel])

  var indexRange = conf.getInt(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val maxTreeNum = conf.getInt(MLConf.ML_GBDT_TREE_NUM, MLConf.DEFAULT_ML_GBDT_TREE_NUM)
  val maxTreeDepth = conf.getInt(MLConf.ML_GBDT_TREE_DEPTH, MLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
  val splitNum = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM, MLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
  val featSampleRatio = conf.getFloat(MLConf.ML_GBDT_SAMPLE_RATIO, MLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)
  val cateFeatStr = conf.get(MLConf.ML_GBDT_CATE_FEAT, MLConf.DEFAULT_ML_GBDT_CATE_FEAT)
  val cateFeatNum = if (cateFeatStr.contains(",")) cateFeatStr.split(",").length else 1

  val maxTNodeNum: Int = Maths.pow(2, maxTreeDepth) - 1

  // # parameter server
  val psNumber = conf.getInt(AngelConf.ANGEL_PS_NUMBER, 1)
  val workerNumber = conf.getInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM, 1)

  // adjust feature number to ensure the parameter partition
  if (indexRange % psNumber != 0) {
    indexRange = (indexRange / psNumber + 1) * psNumber
    conf.setInt(MLConf.ML_FEATURE_INDEX_RANGE, indexRange)
    LOG.info(s"PS num: $psNumber, true feat num: $indexRange")
  }

  val sampleFeatNum: Int = (indexRange * featSampleRatio).toInt

  // Matrix 1: quantile sketch
  val sketch = PSModel(SKETCH_MAT, 1, indexRange * splitNum, 1, indexRange * splitNum / psNumber)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
    .setNeedSave(false)
  addPSModel(SKETCH_MAT, sketch)

  //  private val maxQSketchSize = 1 + SketchUtils.needBufferCapacity(HeapQuantileSketch.DEFAULT_K, numFeatNnz.toLong)
  //  private val sketchMat = PSModel(GBDTModel.SKETCH_MAT, batchSize, maxQSketchSize, batchSize / psNumber, maxQSketchSize)
  //    .setRowType(RowType.T_FLOAT_DENSE)
  //    .setOplogType("T_FLOAT_DENSE")
  //    .setNeedSave(false)
  //  addPSModel(GBDTModel.SKETCH_MAT, sketchMat)

  // Matrix 2: sampled feature
  val featSample = PSModel(FEAT_SAMPLE_MAT, maxTreeNum, sampleFeatNum, 1, sampleFeatNum / psNumber)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("T_INT_DENSE")
    .setNeedSave(false)
  addPSModel(FEAT_SAMPLE_MAT, featSample)

  val histMats: Array[PSModel] = new Array[PSModel](maxTNodeNum)
  // Matrix 3: gradient and hess histogram, one for each node
  for (nid <- 0 until maxTNodeNum) {
    val histMat = PSModel(GRAD_HIST_MAT_PREFIX + nid,
      1, 2 * this.splitNum * sampleFeatNum, 1, 2 * this.splitNum * sampleFeatNum / psNumber)
      .setRowType(RowType.T_DOUBLE_DENSE)
      .setOplogType("T_DOUBLE_DENSE")
      .setNeedSave(false)
    addPSModel(GRAD_HIST_MAT_PREFIX + nid, histMat)
    histMats(nid) = histMat
  }

  // Matrix 4: active tree nodes
  val activeTNodes = PSModel(ACTIVE_NODE_MAT, 1, maxTNodeNum, 1, maxTNodeNum / psNumber)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("T_INT_DENSE")
    .setNeedSave(false)
  addPSModel(ACTIVE_NODE_MAT, activeTNodes)

  // Matrix 5: split feature
  val splitFeat = new PSModel(SPLIT_FEAT_MAT, maxTreeNum, maxTNodeNum, maxTreeNum, maxTNodeNum / psNumber)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("T_INT_DENSE")
  addPSModel(SPLIT_FEAT_MAT, splitFeat)

  // Matrix 6: split value
  val splitValue = PSModel(SPLIT_VALUE_MAT, maxTreeNum, maxTNodeNum, maxTreeNum, maxTNodeNum / psNumber)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
  addPSModel(SPLIT_VALUE_MAT, splitValue)

  // Matrix 7: split loss gain
  val splitGain = PSModel(SPLIT_GAIN_MAT, maxTreeNum, maxTNodeNum, maxTreeNum, maxTNodeNum / psNumber)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
    .setNeedSave(false)
  addPSModel(SPLIT_GAIN_MAT, splitGain)

  // Matrix 8: node's grad stats
  val nodeGradStats = PSModel(NODE_GRAD_MAT, maxTreeNum, 2 * maxTNodeNum, maxTreeNum, 2 * maxTNodeNum / psNumber)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
    .setNeedSave(false)
  addPSModel(NODE_GRAD_MAT, nodeGradStats)

  // Matrix 9: node's predict value
  val nodePred = PSModel(NODE_PRED_MAT, maxTreeNum, maxTNodeNum, maxTreeNum, maxTNodeNum / psNumber)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
  addPSModel(NODE_PRED_MAT, nodePred)

  // Matrix 10: categorical feature
  val featCategory = PSModel(FEAT_CATEGORY_MAT, workerNumber, cateFeatNum * splitNum, 1, cateFeatNum * splitNum)
    .setRowType(RowType.T_DOUBLE_DENSE)
    .setOplogType("T_DOUBLE_DENSE")
    .setNeedSave(false)
  addPSModel(FEAT_CATEGORY_MAT, featCategory)

  super.setSavePath(conf)
  super.setLoadPath(conf)

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val predict = new MemoryDataBlock[PredictResult](-1)

    val splitFeatVecs: Array[IntIntVector] = new Array[IntIntVector](this.maxTreeNum)
    val splitValueVecs: Array[IntDoubleVector] = new Array[IntDoubleVector](this.maxTreeNum)
    val nodePredVecs: Array[IntDoubleVector] = new Array[IntDoubleVector](this.maxTreeNum)

    (0 until this.maxTreeNum).foreach { treeIdx =>
      splitFeatVecs(treeIdx) = this.splitFeat.getRow(treeIdx).asInstanceOf[IntIntVector]
      splitValueVecs(treeIdx) = this.splitValue.getRow(treeIdx).asInstanceOf[IntDoubleVector]
      nodePredVecs(treeIdx) = this.nodePred.getRow(treeIdx).asInstanceOf[IntDoubleVector]

      LOG.info(s"Tree[$treeIdx] split feature: ${splitFeatVecs(treeIdx).getStorage.getValues.mkString(",")}")
      LOG.info(s"Tree[$treeIdx] split value: ${splitValueVecs(treeIdx).getStorage.getValues.mkString(",")}")
      LOG.info(s"Tree[$treeIdx] node predictions: ${nodePredVecs(treeIdx).getStorage.getValues.mkString(",")}")
    }

    dataSet.resetReadIndex
    val lr: Double = conf.getFloat(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEARN_RATE.asInstanceOf[Float])
    var posTrue: Int = 0
    var posNum: Int = 0
    var negTrue: Int = 0
    var negNum: Int = 0

    (0 until dataSet.size).foreach { idx =>
      val instance = dataSet.read
      val x: IntDoubleVector = instance.getX.asInstanceOf[IntDoubleVector]
      val y: Double = instance.getY
      var pred: Double = 0

      (0 until this.maxTreeNum).foreach { treeIdx =>
        var nid: Int = 0
        var splitFeat: Int = splitFeatVecs(treeIdx).get(nid)
        var splitValue: Double = splitValueVecs(treeIdx).get(nid)
        var curPred: Double = nodePredVecs(treeIdx).get(nid)

        while (splitFeat != -1 && nid < splitFeatVecs(treeIdx).getDim) {
          nid = if (x.get(splitFeat) <= splitValue)
            2 * nid + 1 else 2 * nid + 2
          if (nid < splitFeatVecs(treeIdx).getDim) {
            splitFeat = splitFeatVecs(treeIdx).get(nid)
            splitValue = splitValueVecs(treeIdx).get(nid)
            curPred = nodePredVecs(treeIdx).get(nid)
          }
        }
        pred += lr * curPred
      }

      predict.put(new GBDTPredictResult(idx, y, pred))
      LOG.debug(s"instance[$idx]: label[$y], pred[$pred]")

      if (y > 0) {
        posNum += 1
        if (y * pred > 0) posTrue += 1
      } else {
        negNum += 1
        if (y * pred >= 0) negTrue += 1
      }
    }

    LOG.info(s"Positive accuracy: ${posTrue.toDouble / posNum.toDouble}, " +
      s"negative accuracy: ${negTrue.toDouble / negNum.toDouble}")
    predict
  }

}

case class GBDTPredictResult(sid: Long, pred: Double, label: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText: String = {
    df.format(sid) + separator + format.format(pred) + separator + df.format(label)
  }
}
