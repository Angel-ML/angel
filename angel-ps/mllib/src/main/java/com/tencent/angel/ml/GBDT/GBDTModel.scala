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

import java.io.DataOutputStream
import java.text.DecimalFormat

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.GBDT.GBDTModel._
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{TDoubleVector, TIntVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.utils.MathUtils
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

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

  def apply(conf: Configuration) = {
    new GBDTModel(conf)
  }

  def apply(ctx:TaskContext, conf: Configuration) = {
    new GBDTModel(ctx, conf)
  }
}

class GBDTModel(_ctx: TaskContext, conf: Configuration) extends MLModel(_ctx) {

  def this(conf: Configuration) = {
    this(null, conf)
  }

  var LOG = LogFactory.getLog(classOf[GBDTModel])

  var featNum = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  val featNonzero = conf.getInt(MLConf.ML_FEATURE_NNZ, MLConf.DEFAULT_ML_FEATURE_NNZ)
  val maxTreeNum = conf.getInt(MLConf.ML_GBDT_TREE_NUM, MLConf.DEFAULT_ML_GBDT_TREE_NUM)
  val maxTreeDepth = conf.getInt(MLConf.ML_GBDT_TREE_DEPTH, MLConf.DEFAULT_ML_GBDT_TREE_DEPTH)
  val splitNum = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM, MLConf.DEFAULT_ML_GBDT_SPLIT_NUM)
  val featSampleRatio = conf.getFloat(MLConf.ML_GBDT_SAMPLE_RATIO, MLConf.DEFAULT_ML_GBDT_SAMPLE_RATIO)

  val maxTNodeNum: Int = MathUtils.pow(2, maxTreeDepth) - 1

  // # parameter server
  val psNumber = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, 1)

  // adjust feature number to ensure the parameter partition
  if (featNum % psNumber != 0) {
    featNum = (featNum / psNumber + 1) * psNumber
    conf.setInt(MLConf.ML_FEATURE_NUM, featNum)
    LOG.info(s"PS num: ${psNumber}, true feat num: ${featNum}")
  }

  val sampFeatNum: Int = (featNum * featSampleRatio).toInt

  // Matrix 1: quantile sketch
  val sketch = new PSModel[TDoubleVector](SKETCH_MAT, 1, featNum * splitNum, 1, featNum * splitNum / psNumber)
  sketch.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  sketch.setOplogType("DENSE_DOUBLE")
  addPSModel(SKETCH_MAT, sketch)

  // Matrix 2: sampled feature
  val featSample = new PSModel[TIntVector](FEAT_SAMPLE_MAT, maxTreeNum, sampFeatNum, 1, sampFeatNum / psNumber)
  featSample.setRowType(MLProtos.RowType.T_INT_DENSE)
  featSample.setOplogType("DENSE_INT")
  addPSModel(FEAT_SAMPLE_MAT, featSample)

  val histMats: Array[PSModel[TDoubleVector]] = new Array[PSModel[TDoubleVector]](maxTNodeNum)
  // Matrix 3: gradient and hess histogram, one for each node
  for (nid <- 0 until maxTNodeNum) {
    val histMat = new PSModel[TDoubleVector](GRAD_HIST_MAT_PREFIX + nid, 1, 2 * this.splitNum * sampFeatNum, 1, 2 * this.splitNum * sampFeatNum / psNumber)
    histMat.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
    histMat.setOplogType("DENSE_DOUBLE")
    addPSModel(GRAD_HIST_MAT_PREFIX + nid, histMat)
    histMats(nid) = histMat
  }

  // Matrix 4: active tree nodes
  val activeTNodes = new PSModel[TIntVector](ACTIVE_NODE_MAT, 1, maxTNodeNum, 1, maxTNodeNum / psNumber)
  activeTNodes.setRowType(MLProtos.RowType.T_INT_DENSE)
  activeTNodes.setOplogType("DENSE_INT")
  addPSModel(ACTIVE_NODE_MAT, activeTNodes)

  // Matrix 5: split feature
  val splitFeat = new PSModel[TIntVector](SPLIT_FEAT_MAT, maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitFeat.setRowType(MLProtos.RowType.T_INT_DENSE)
  splitFeat.setOplogType("DENSE_INT")
  addPSModel(SPLIT_FEAT_MAT, splitFeat)

  // Matrix 6: split value
  val splitValue = new PSModel[TDoubleVector](SPLIT_VALUE_MAT, maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitValue.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  splitValue.setOplogType("DENSE_DOUBLE")
  addPSModel(SPLIT_VALUE_MAT, splitValue)

  // Matrix 7: split loss gain
  val splitGain = new PSModel[TDoubleVector](SPLIT_GAIN_MAT, maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  splitGain.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  splitGain.setOplogType("DENSE_DOUBLE")
  addPSModel(SPLIT_GAIN_MAT, splitGain)

  // Matrix 8: node's grad stats
  val nodeGradStats = new PSModel[TDoubleVector](NODE_GRAD_MAT, maxTreeNum, 2 * maxTNodeNum, 1, 2 * maxTNodeNum / psNumber)
  nodeGradStats.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  nodeGradStats.setOplogType("DENSE_DOUBLE")
  addPSModel(NODE_GRAD_MAT, nodeGradStats)

  // Matrix 9: node's predict value
  val nodePred = new PSModel[TDoubleVector](NODE_PRED_MAT, maxTreeNum, maxTNodeNum, 1, maxTNodeNum / psNumber)
  nodePred.setRowType(MLProtos.RowType.T_DOUBLE_DENSE)
  nodePred.setOplogType("DENSE_DOUBLE")
  addPSModel(NODE_PRED_MAT, nodePred)

  setLoadPath(conf)
  setSavePath(conf)

  def init(ctx: TaskContext) = {
  }

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    if (path == null) return
    //sketch.setSavePath(path + "/" + sketch.modelName)
    //featSample.setSavePath(path + "/" + featSample.modelName)
    //for (nid <- 0 until maxTNodeNum) {
    //  histMats(nid).setSavePath(path + "/" + GRAD_HIST_MAT_PREFIX + nid)
    //}
    //activeTNodes.setSavePath(path + "/" + activeTNodes.modelName)
    splitFeat.setSavePath(path + "/" + splitFeat.modelName)
    splitValue.setSavePath(path + "/" + splitValue.modelName)
    //splitGain.setSavePath(path + "/" + splitGain.modelName)
    //nodeGradStats.setSavePath(path + "/" + nodeGradStats.modelName)
    nodePred.setSavePath(path + "/" + nodePred.modelName)
  }

  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    if (path == null) return
    //sketch.setLoadPath(path)
    //featSample.setLoadPath(path)
    //for (nid <- 0 until maxTNodeNum) {
    //  histMats(nid).setLoadPath(path)
    //}
    //activeTNodes.setLoadPath(path)
    splitFeat.setLoadPath(path)
    splitValue.setLoadPath(path)
    //splitGain.setLoadPath(path)
    //nodeGradStats.setLoadPath(path)
    nodePred.setLoadPath(path)
  }

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val predict = new MemoryDataBlock[PredictResult](-1)

    val splitFeatVecs: Array[TIntVector] = new Array[TIntVector](this.maxTreeNum)
    val splitValueVecs: Array[TDoubleVector] = new Array[TDoubleVector](this.maxTreeNum)
    val nodePredVecs: Array[TDoubleVector] = new Array[TDoubleVector](this.maxTreeNum)

    for (treeIdx: Int <- 0 until this.maxTreeNum) {
      splitFeatVecs(treeIdx) = this.splitFeat.getRow(treeIdx)
      splitValueVecs(treeIdx) = this.splitValue.getRow(treeIdx)
      nodePredVecs(treeIdx) = this.nodePred.getRow(treeIdx)
      LOG.info(s"Tree[${treeIdx}] split feature: ${splitFeatVecs(treeIdx).getValues.mkString(",")}")
      LOG.info(s"Tree[${treeIdx}] split value: ${splitValueVecs(treeIdx).getValues.mkString(",")}")
      LOG.info(s"Tree[${treeIdx}] node predictions: ${nodePredVecs(treeIdx).getValues.mkString(",")}")
    }

    dataSet.resetReadIndex()
    val lr: Double = conf.getFloat(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE.asInstanceOf[Float])
    var posTrue: Int = 0
    var posNum: Int = 0
    var negTrue: Int = 0
    var negNum: Int = 0

    for (idx: Int <- 0 until dataSet.getTotalElemNum) {
      val instance = dataSet.read
      val x: TDoubleVector = instance.getX.asInstanceOf[TDoubleVector]
      val y: Double = instance.getY
      var pred: Double = 0

      for (treeIdx: Int <- 0 until this.maxTreeNum) {
        var nid: Int = 0
        var splitFeat: Int = splitFeatVecs(treeIdx).get(nid)
        var splitValue: Double = splitValueVecs(treeIdx).get(nid)
        var curPred: Double = nodePredVecs(treeIdx).get(nid)

        while (-1 != splitFeat && nid < splitFeatVecs(treeIdx).getDimension) {
          if (x.get(splitFeat) <= splitValue) {
            nid = 2 * nid + 1
          }
          else {
            nid = 2 * nid + 2
          }
          if (nid < splitFeatVecs(treeIdx).getDimension) {
            splitFeat = splitFeatVecs(treeIdx).get(nid)
            splitValue = splitValueVecs(treeIdx).get(nid)
            curPred = nodePredVecs(treeIdx).get(nid)
          }
        }
        pred += lr * curPred
      }

      predict.put(new GBDTPredictResult(y, pred))
      LOG.info(s"instance[${idx}]: label[${y}], pred[${pred}]")
      if (y > 0) {
        posNum += 1
        if (y * pred > 0) posTrue += 1
      } else {
        negNum += 1
        if (y * pred >= 0) negTrue += 1
      }
    }
    LOG.info(s"Positive accuracy: ${posTrue.toDouble/posNum.toDouble}, " +
      s"negative accuracy: ${negTrue.toDouble/negNum.toDouble}")
    predict
  }

}

class GBDTPredictResult(label: Double, pred: Double) extends PredictResult {
  val format = new DecimalFormat("0.000000")

  override
  def writeText(output: DataOutputStream): Unit = {
    output.writeBytes(label + PredictResult.separator + format.format(pred))
  }
}
