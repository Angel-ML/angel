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

package com.tencent.angel.ml.classification.svm

import java.io.DataOutputStream
import java.text.DecimalFormat

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.classification.svm.SVMModel._
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.utils.MathUtils
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object SVMModel {
  val SVM_WEIGHT_MAT = "svm.weight.mat"
  val SVM_LOSS_MAT = "svm.loss.mat"
}

class SVMModel(_ctx: TaskContext, conf: Configuration) extends MLModel(_ctx) {
  var LOG = LogFactory.getLog(classOf[SVMModel])

  def this(conf: Configuration) = {
    this(null, conf)
  }

  // Feature number
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // The feature weight vector, stored on PS
  val weight = new PSModel[TDoubleVector](SVM_WEIGHT_MAT, 1, feaNum)
  weight.setAverage(true)
  addPSModel(SVM_WEIGHT_MAT, weight)

  // Iteration number
  val epochNum = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  // A vector stores total validation losses, sotred on PS
  val loss = new PSModel[DenseDoubleVector](SVM_LOSS_MAT, 1, epochNum)
  addPSModel(SVM_LOSS_MAT, loss)
  setLoadPath(conf)
  setSavePath(conf)

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    if (path != null)
      weight.setSavePath(path)
  }

  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    if (path != null)
      weight.setLoadPath(path)
  }

  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val predict = new MemoryDataBlock[PredictResult](-1)
    val wVector = weight.getRow(0)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.getTotalElemNum) {
      val instance = dataSet.read
      val id = instance.getY
      val dot = wVector.dot(instance.getX)
      val sig = MathUtils.sigmoid(dot)
      predict.put(new svmPredictResult(id, dot, sig))
    }
    predict
  }
}

class svmPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {
  val format = new DecimalFormat("0.000000");

  override
  def writeText(output: DataOutputStream): Unit = {

    output.writeBytes(id + PredictResult.separator + format.format(dot) +
      PredictResult.separator + format.format(sig))
  }
}