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

package com.tencent.angel.ml.classification.sparselr


import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector, SparseDoubleVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


class SparseLogisticRegressionModel(conf: Configuration, ctx: TaskContext) extends MLModel {

  val W = "w"
  val T = "t"
  val Z = "z"
  val LOSS = "loss"
  val AUC = "auc"

  val BUCKET_NUM = "bucket_num"

  def this(conf: Configuration) = {
    this(conf, null)
  }

  // Number of dimensions
  val feaNum = conf.getInt(ML_FEATURE_NUM, DEFAULT_ML_FEATURE_NUM)
  // Bucket number for calculation of AUC
  val bucketNum = conf.getInt(BUCKET_NUM, 10000)

  val w = new PSModel[DenseDoubleVector](ctx, W, 1, feaNum)
  val z = new PSModel[SparseDoubleVector](ctx, Z, 1, feaNum)
  z.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE)
  val t = new PSModel[DenseDoubleVector](ctx, T, 1, 1)
  val loss = new PSModel[DenseDoubleVector](ctx, LOSS, 1, 1)
  val auc  = new PSModel[DenseIntVector](ctx, AUC, 1, bucketNum * 2)
  auc.setRowType(RowType.T_INT_DENSE)
  auc.setOplogType("DENSE_INT")

  addPSModel(w)
  addPSModel(z)
  addPSModel(t)
  addPSModel(loss)
  addPSModel(auc)
  setLoadPath(conf)
  setSavePath(conf)

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    // save w matrix to HDFS
    if(w!=null)
      w.setSavePath(path)
  }

  override
  def setLoadPath(conf: Configuration): Unit = {

  }


  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }

  def clean(): Unit = {
    w.zero()
//    z.zero()
    t.zero()
    loss.zero()
//    auc.zero()
  }

}
