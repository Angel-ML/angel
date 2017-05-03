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

package com.tencent.angel.ml.algorithm.classification.svm

import java.io.{DataOutputStream}
import java.text.DecimalFormat

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.MathUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.ml.algorithm.classification.svm.SVMModel._

object SVMModel {
  val SVM_WEIGHT_MAT = "svm.weight"
}

class SVMModel(ctx: TaskContext, conf: Configuration) extends AlgorithmModel {
  var LOG = LogFactory.getLog(classOf[SVMModel])

  def this(conf: Configuration) = {
    this(null, conf)
  }

  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  val weight = new PSModel[TDoubleVector](ctx, SVM_WEIGHT_MAT, 1, feaNum)

  addPSModel(SVM_WEIGHT_MAT, weight)

  var localWeight: TDoubleVector = _

  def pullWeightFromPS() = {
    localWeight = weight.getRow( 0)
  }

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    weight.setSavePath(path)
  }

  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    weight.setLoadPath(path)
  }

  override
  def predict(dataSet: Storage[LabeledData]): Storage[PredictResult] = {
    val predict = new MemoryStorage[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.getTotalElemNum) {
      val instance = dataSet.read
      val id = instance.getY
      val dot = localWeight.dot(instance.getX)
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