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


import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector, SparseDoubleVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.MathUtils
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


class SparseLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  val W = "w"
  val T = "t"
  val Z = "z"
  val LOSS = "loss"
  val AUC = "auc"

  val BUCKET_NUM = "bucket_num"

  // Number of dimensions
  val feaNum = conf.getInt(ML_FEATURE_NUM, DEFAULT_ML_FEATURE_NUM)
  // Bucket number for calculation of AUC
  val bucketNum = conf.getInt(BUCKET_NUM, 10000)

  val w = PSModel[DenseDoubleVector](W, 1, feaNum).setNeedSave(false)
  val z = PSModel[SparseDoubleVector](Z, 1, feaNum).setRowType(MLProtos.RowType.T_DOUBLE_SPARSE)
  val t = PSModel[DenseDoubleVector](T, 1, 1).setNeedSave(false)
  val loss = PSModel[DenseDoubleVector](LOSS, 1, 1).setNeedSave(false)
  val auc  = PSModel[DenseIntVector](AUC, 1, bucketNum * 2)
    .setNeedSave(false)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")

  addPSModel(w)
  addPSModel(z)
  addPSModel(t)
  addPSModel(loss)
  addPSModel(auc)

  super.setSavePath(conf)
  super.setLoadPath(conf)



  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val weight = this.z.getRow(0)
    val predict = new MemoryDataBlock[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val id = instance.getY
      val dot = weight.dot(instance.getX)
      val sig = MathUtils.sigmoid(dot)
      predict.put(new LRPredictResult(id, dot, sig))
    }
    predict
  }

  def clean(): Unit = {
    w.zero()
    t.zero()
    loss.zero()
  }

}


class LRPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {
  override def getText(): String = {
    (id + separator + format.format(dot) + separator + format.format(sig))
  }
}
