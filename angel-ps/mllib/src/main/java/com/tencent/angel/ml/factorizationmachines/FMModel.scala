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

package com.tencent.angel.ml.factorizationmachines

import java.util

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object FMModel {
  def apply(conf: Configuration) =  {
    new FMModel(conf)
  }

  def apply(conf: Configuration, ctx: TaskContext) = {
    new FMModel(conf, ctx)
  }
}


class FMModel (conf: Configuration = null, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[FMModel])

  val FM_W0 = "fm_w0"
  val FM_W = "fm_w"
  val FM_V = "fm_v"
  val FM_OBJ = "fm.obj"

  // Feature number of data
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val rank = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)

  // The w0 weight vector, stored on PS
  val w0 = PSModel[DenseDoubleVector](FM_W0, 1, 1).setAverage(true)
  // The w weight vector, stored on PS
  val w = PSModel[DenseDoubleVector](FM_W, 1, feaNum).setAverage(true)
  // The v weight vector, stored on PS
  val v = PSModel[DenseDoubleVector](FM_V, feaNum, rank).setAverage(true)

  addPSModel(w0)
  addPSModel(w)
  addPSModel(v)

  super.setSavePath(conf)
  super.setLoadPath(conf)



  /**
    *
    * @param dataSet
    * @return
    */
  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = ???

  def pullFromPS(vIndexs: Array[Int]) = (w0.getRow(0), w.getRow(0), v.getRows(vIndexs))

  def pushToPS(update0: DenseDoubleVector, update1: DenseDoubleVector, update2: util
  .List[DenseDoubleVector]) = {
    w0.increment(0, update0)
    w.increment(0, update1)
    for (i <- 0 until feaNum) {
      v.increment(i, update2.get(i))
    }

    w0.clock().get()
    w.clock().get()
    v.clock().get()
  }
}
