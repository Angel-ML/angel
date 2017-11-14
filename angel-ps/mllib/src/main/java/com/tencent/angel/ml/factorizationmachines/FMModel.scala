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

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

/*
 * Factorization machines model
 */
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
  val FM_OBJ = "fm_evaluate"

  // Feature number of data
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // Rank of each feature vector
  val rank = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)

  // The w0 weight vector, stored on PS
  val w0 = PSModel(FM_W0, 1, 1).setAverage(true).setOplogType("SPARSE_DOUBLE")
  // The w weight vector, stored on PS
  val w = PSModel(FM_W, 1, feaNum).setAverage(true).setOplogType("SPARSE_DOUBLE")
  // The v weight vector, stored on PS
  val v = PSModel(FM_V, feaNum, rank).setAverage(true).setOplogType("SPARSE_DOUBLE")

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

  /** Pull w0, w, v from PS
    *
    * @param vIndexs
    * @return
    */
  def pullFromPS(vIndexs: RowIndex) = {
    (w0.getRow(0).asInstanceOf[DenseDoubleVector], w.getRow(0).asInstanceOf[DenseDoubleVector], v.getRows(vIndexs, -1))
  }

  /** Push update values of w0, w, v
    *
    * @param update0: update values of w0
    * @param update1: update values of w
    * @param update2: update values of v
    * @return
    */
  def pushToPS(update0: DenseDoubleVector,
               update1: DenseDoubleVector,
               update2: mutable.Map[Int, TVector]) = {
    w0.increment(0, update0)
    w.increment(0, update1)

    for (vec <- update2) {
      v.increment(vec._1, vec._2)
    }

    w0.syncClock()
    w.syncClock()
    v.syncClock()
  }
}
