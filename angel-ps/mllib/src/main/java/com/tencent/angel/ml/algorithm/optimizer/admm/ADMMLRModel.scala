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

package com.tencent.angel.ml.algorithm.optimizer.admm

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.conf.MLConf._
import com.tencent.angel.ml.algorithm.optimizer.admm.ADMMLRModel._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector}
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import com.tencent.angel.psagent.matrix.MatrixClient
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.Storage
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


object ADMMLRModel {

  val L1_NORM = "angel.admmlr.norm"
  val LBFGS_ITER = "angel.admmlr.lbfgs.iter"
  val RHO = "angel.admmlr.rho"
  val BUCKET_NUM = "angel.admmlr.bucket.num"

  val W = "w"
  val T = "t"
  val Z = "z"
  val LOSS = "loss"
  val AUC = "auc"
}


class ADMMLRModel(conf: Configuration, ctx: TaskContext) extends AlgorithmModel {

  def this(conf: Configuration) = {
    this(conf, null)
  }

  // Initializing parameters

  // l1 norm for admm algorithm
  val l1_NORM = conf.getDouble(L1_NORM, 0.0)
  // Iteration number for lbfgs algorithm
  val lBFGS_ITER = conf.getInt(LBFGS_ITER, 5)
  val epoch = conf.getInt(ML_EPOCH_NUM, 10)

  // Rho algorithm
  val rho = conf.getDouble(RHO, 0.0)
  val feaNum = conf.getInt(ML_FEATURE_NUM, 0)
  // Bucket number for calculation of AUC
  val bucketNum = conf.getInt(BUCKET_NUM, 10000)

  val w = new PSModel[DenseDoubleVector](W, 1, feaNum)
  val z = new PSModel[DenseDoubleVector](Z, 1, feaNum)
  val t = new PSModel[DenseDoubleVector](T, 1, 1)
  val loss = new PSModel[DenseDoubleVector](LOSS, 1, 1)
  val auc  = new PSModel[DenseIntVector](AUC, 1, bucketNum * 2)
  auc.setRowType(RowType.T_INT_DENSE)
  auc.setOplogType("DENSE_INT")

  addPSModel(w)
  addPSModel(z)
  addPSModel(t)
  addPSModel(loss)
  addPSModel(auc)

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    // save w matrix to HDFS
    w.setSavePath(path)
  }

  override
  def predict(dataSet: Storage[LabeledData]): Storage[PredictResult] = {
    null
  }

  var _wMat : MatrixClient = null
  var _zMat : MatrixClient = null
  var _tMat : MatrixClient = null
  var _lossMat : MatrixClient = null
  var _aucMat  : MatrixClient = null

  /**
    * Reference for the w matrix on PS servers
    * @return Matrix client for w matrix
    */
  def wMat = {
    if (_wMat == null)
      _wMat = ctx.getMatrix(w.getName)
    _wMat
  }

  /**
    * Reference for the z matrix on PS servers
    * @return Matrix Client for z matrix
    */
  def zMat = {
    if (_zMat == null)
      _zMat = ctx.getMatrix(z.getName)
    _zMat
  }

  /**
    *
    * @return
    */
  def tMat = {
    if (_tMat == null)
      _tMat = ctx.getMatrix(t.getName)
    _zMat
  }

  def lossMat = {
    if (_lossMat == null)
      _lossMat = ctx.getMatrix(loss.getName)
    _lossMat
  }

  def aucMat = {
    if (_aucMat == null)
      _aucMat = ctx.getMatrix(auc.getName)
    _aucMat
  }

}
