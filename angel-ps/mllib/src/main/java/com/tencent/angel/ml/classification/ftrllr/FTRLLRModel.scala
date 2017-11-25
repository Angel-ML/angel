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
package com.tencent.angel.ml.classification.ftrllr

import java.text.DecimalFormat

import com.tencent.angel.ml.classification.lr.LRPredictResult
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleVector, SparseLongKeyDoubleVector, TDoubleVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.optimizer.ftrl.FTRL
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

object FTRLLRModel {
  def apply(conf: Configuration) = {
    new FTRLLRModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new FTRLLRModel(conf, ctx)
  }
}

class FTRLLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[FTRLLRModel])

  val FTRL_LR_Z = "ftrl_lr_z"
  val FTRL_LR_N = "ftrl_lr_n"

  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val batchSize: Int = conf.getInt(MLConf.ML_FTRL_BATCH_SIZE, MLConf.DEFAULT_ML_FTRL_BATCH_SIZE)
  val spRatio: Double = conf.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)
  val alpha: Double = conf.getDouble(MLConf.ML_FTRL_ALPHA, MLConf.DEFAULT_ML_FTRL_ALPHA)
  val beta: Double = conf.getDouble(MLConf.ML_FTRL_BETA, MLConf.DEFAULT_ML_FTRL_BETA)
  val lambda1: Double = conf.getDouble(MLConf.ML_FTRL_LAMBDA1, MLConf.DEFAULT_ML_FTRL_LAMBDA1)
  val lambda2: Double = conf.getDouble(MLConf.ML_FTRL_LAMBDA2, MLConf.DEFAULT_ML_FTRL_LAMBDA2)


  // PSModel z, z^t = \Sigma_{s=1}^t g - \Sigma_{s=1}^t \delta^s \cdot w
  val zMat = PSModel(FTRL_LR_Z, 1, feaNum)
    .setAverage(true)
    .setRowType(MLProtos.RowType.T_DOUBLE_SPARSE)

  // PSModel n, n^t = \Sigma_{i=1}^t g_i^2
  val nMat = PSModel(FTRL_LR_N, 1, feaNum).setAverage(true)
    .setAverage(true)
    .setRowType(MLProtos.RowType.T_DOUBLE_SPARSE)

  addPSModel(FTRL_LR_Z, zMat)
  addPSModel(FTRL_LR_N, nMat)

  setSavePath(conf)
  setLoadPath(conf)

  /**
    *
    * @param dataSet
    * @return
    */
  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val start = System.currentTimeMillis()
    val z = zMat.getRow(0).asInstanceOf[TDoubleVector]
    val n = nMat.getRow(0).asInstanceOf[TDoubleVector]
    val wVector = z match {
      case _: DenseDoubleVector => new DenseDoubleVector(z.getDimension)
      case _: SparseDoubleVector => new SparseDoubleVector(z.getDimension)
      case _: SparseLongKeyDoubleVector => new SparseLongKeyDoubleVector(z.getDimension)
    }
    FTRL.computeW(z, n, wVector, alpha, beta, lambda1, lambda2)
    val cost = System.currentTimeMillis() - start
    LOG.info(s"pull FTRLLR Model from PS cost $cost ms.")

    val predict = new MemoryDataBlock[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val id = instance.getY
      val dot = wVector.dot(instance.getX)
      val sig = Maths.sigmoid(dot)
      predict.put(new FTRLLRPredictResult(id, dot, sig))
    }
    predict
  }
}

class FTRLLRPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText(): String = {
    df.format(id) + separator + format.format(dot) + separator + format.format(sig)
  }
}
