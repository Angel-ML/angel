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

package com.tencent.angel.ml.classification2.lr

import java.text.DecimalFormat
import java.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.math.{TUpdate, TVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.{RandomNormal, SoftThreshold}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.optimizer2.OptModel
import com.tencent.angel.ml.optimizer2.lossfuncs.LogisticLoss
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.math.Numeric
import scala.reflect.runtime.universe._
import collection.JavaConversions._

/**
  * LR model
  *
  */

object LRModel {
  def apply(conf: Configuration) = {
    new LRModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new LRModel(conf, ctx)
  }
}

class LRModel(conf: Configuration, _ctx: TaskContext = null) extends OptModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[LRModel])
  private val (weight, intercept) = ("lr_weight", "lr_intercept")

  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)
  val modelType: RowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, MLConf.DEFAULT_ML_MODEL_TYPE))

  addPSModel(weight, PSModel(weight, 1, indexRange, -1, -1, modelSize).setAverage(true).setRowType(modelType))
  addPSModel(intercept, PSModel(intercept, 1, 1, -1, -1, 1).setAverage(true).setRowType(modelType))

  setSavePath(conf)
  setLoadPath(conf)

  def initModels[N: Numeric : TypeTag](indexes: Array[N]): Unit = {
    val weightParams = getPSModels.get(weight)
    val biasParams = getPSModels.get(intercept)

    if (ctx.getTaskId.getIndex == 0) {
      LOG.info(s"${ctx.getTaskId} is in charge of intial model, start ...")
      val vStddev = 0.0001
      initBiasModel(biasParams)
      LOG.info(s"bias initial finished!")

      initVectorModel(weightParams, indexes, vStddev)
      LOG.info(s"w initial finished!")

      LOG.info(s"${ctx.getTaskId} finished intial model!")
    }

    LOG.info(s"Now begin to syncClock $weight, $intercept in the frist time!")
    weightParams.syncClock()
    biasParams.syncClock()
  }

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val start = System.currentTimeMillis()

    val wVector = getPSModels.get(weight).getRow(0)
    val bias = getBias(getPSModels.get(intercept).getRow(0))

    val cost = System.currentTimeMillis() - start
    LOG.info(s"pull LR Model from PS cost $cost ms.")

    val predict = new MemoryDataBlock[PredictResult](-1)

    dataSet.resetReadIndex()
    for (_: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val dot = wVector.dot(instance.getX) + bias
      val sig = Maths.sigmoid(dot)
      predict.put(new LRPredictResult(instance.getY, dot, sig))
    }

    predict
  }

  override def calLossAndUpdateGrad(x: TVector, y: Double,
                                    params: util.HashMap[String, TUpdate],
                                    gradient: util.HashMap[String, TUpdate]): Double = {
    val wVector = params.get(weight).asInstanceOf[TVector]
    val bias = getBias(params.get(intercept))

    val fxValue = wVector.dot(x) + bias

    // - y / (1.0 + Math.exp(y * fxValue))
    val derviationMultipler = LogisticLoss.grad(fxValue, y)

    gradient.get(weight).asInstanceOf[TVector].plusBy(x, derviationMultipler)
    gradient.get(intercept).asInstanceOf[TVector].plusBy(0, derviationMultipler)

    LogisticLoss(fxValue, y)
  }

  override def calPredAndLoss(x: TVector, y: Double, params: util.HashMap[String, TUpdate]): (Double, Double) = {
    val wVector = params.get(weight).asInstanceOf[TVector]
    val bias = getBias(params.get(intercept))

    val fxValue = wVector.dot(x) + bias

    1.0 / (1.0 + Math.exp(-fxValue)) -> LogisticLoss(fxValue, y)
  }

  private def getBias(bias: Any): Double = {
    bias.asInstanceOf[TVector].getType match {
      case RowType.T_DOUBLE_SPARSE =>
        bias.asInstanceOf[SparseDoubleVector].get(0)
      case RowType.T_DOUBLE_DENSE =>
        bias.asInstanceOf[DenseDoubleVector].get(0)
      case RowType.T_DOUBLE_SPARSE_LONGKEY =>
        bias.asInstanceOf[SparseLongKeyDoubleVector].get(0)
      case RowType.T_FLOAT_SPARSE =>
        bias.asInstanceOf[SparseFloatVector].get(0)
      case RowType.T_FLOAT_DENSE =>
        bias.asInstanceOf[DenseFloatVector].get(0)
      case RowType.T_FLOAT_SPARSE_LONGKEY =>
        bias.asInstanceOf[SparseLongKeyFloatVector].get(0)
      case _ => throw new AngelException("RowType is not support!")
    }
  }

  override def getIndexFlag: util.HashMap[String, Boolean] = {
    val flag = new util.HashMap[String, Boolean]()
    flag.put(weight, true)
    flag.put(intercept, false)

    flag
  }

  override def psfHook(thresh: mutable.Map[String, Double]): Unit = {
    if (ctx.getTaskId.getIndex == 0 && thresh != null) {
      getPSModels.foreach { case (name: String, psm: PSModel) =>
        if (getIndexFlag.get(name)) {
          (0 until psm.row).foreach { idx =>
            psm.update(new SoftThreshold(psm.getMatrixId(), idx, thresh(name)))
          }
        }
      }
    }

    if (thresh != null) {
      getPSModels.foreach { case (_, psm: PSModel) =>
        psm.syncClock()
      }
    }
  }
}

class LRPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText(): String = {
    df.format(id) + separator + format.format(dot) + separator + format.format(sig)
  }
}
