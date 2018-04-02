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

package com.tencent.angel.ml.classification2.fm

import java.text.DecimalFormat
import java.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.matrix.RowbaseMatrix
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.math.{TMatrix, TUpdate, TVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.SoftThreshold
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.optimizer2.OptModel
import com.tencent.angel.ml.optimizer2.lossfuncs.LogisticLoss
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.math.Numeric
import scala.reflect.runtime.universe._

/**
  * FM model
  *
  */

object FMModel {
  def apply(conf: Configuration) = {
    new FMModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new FMModel(conf, ctx)
  }
}

class FMModel(conf: Configuration, _ctx: TaskContext = null) extends OptModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[FMModel])
  private val (vmat, weight, intercept) = ("fm_vmat", "fm_weight", "fm_intercept")

  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)

  val rank: Int = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)
  val modelType: RowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, RowType.T_DOUBLE_DENSE.toString))
  addPSModel(intercept, PSModel(intercept, 1, 1, -1, -1, 1).setAverage(true).setRowType(modelType))
  addPSModel(weight, PSModel(weight, 1, indexRange, -1, -1, modelSize).setAverage(true).setRowType(modelType))
  private val blockCol = if (rank * indexRange < 1000000) -1 else 1000000 / rank
  addPSModel(vmat, PSModel(vmat, rank, indexRange, rank, blockCol, modelSize).setAverage(true).setRowType(modelType))
  setSavePath(conf)
  setLoadPath(conf)

  def initModels[N: Numeric : TypeTag](indexes: Array[N]): Unit = {
    val vmatParams = getPSModels.get(vmat)
    val weightParams = getPSModels.get(weight)
    val biasParams = getPSModels.get(intercept)

    if (ctx.getTaskId.getIndex == 0) {
      LOG.info(s"${ctx.getTaskId} is in charge of intial model, start ...")
      val vStddev = 0.0001
      initBiasModel(biasParams)
      LOG.info(s"bias initial finished!")

      initVectorModel(weightParams, indexes, vStddev)
      LOG.info(s"w initial finished!")

      initMatrixModel(vmatParams, indexes, vStddev)
      LOG.info(s"v initial finished!")

      LOG.info(s"${ctx.getTaskId} finished intial model!")
    }

    LOG.info(s"Now begin to syncClock $weight, $intercept in the frist time!")
    vmatParams.syncClock()
    weightParams.syncClock()
    biasParams.syncClock()
  }

  private def calModel(x: TVector, vMatrix: TMatrix[_], wVector: TVector, bias: Double): Double = {
    var fxValue = wVector.dot(x) + bias

    x match {
      case x: SparseDummyVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          vMatrix.getRow(f) match {
            case v_row: TDoubleVector =>
              x.getIndices.foreach { idx =>
                val tmp = v_row.get(idx)
                ret1 += tmp
                ret2 += tmp * tmp
              }
            case v_row: TFloatVector =>
              x.getIndices.foreach { idx =>
                val tmp = v_row.get(idx)
                ret1 += tmp
                ret2 += tmp * tmp
              }
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseLongKeyDummyVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          vMatrix.getRow(f) match {
            case v_row: TLongDoubleVector =>
              x.getIndices.foreach { idx =>
                val tmp = v_row.get(idx)
                ret1 += tmp
                ret2 += tmp * tmp
              }
            case v_row: TLongFloatVector =>
              x.getIndices.foreach { idx =>
                val tmp = v_row.get(idx)
                ret1 += tmp
                ret2 += tmp * tmp
              }
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseDoubleVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          val iter = x.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val idx = entry.getIntKey
            val value = entry.getDoubleValue
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseFloatVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          val iter = x.getIndexToValueMap.int2FloatEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val idx = entry.getIntKey
            val value = entry.getFloatValue
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseDoubleSortedVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          x.getIndices.zip(x.getValues).foreach { case (idx, value) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseFloatSortedVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          x.getIndices.zip(x.getValues).foreach { case (idx, value) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseLongKeySortedDoubleVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TLongDoubleVector]
          x.getIndexes.zip(x.getValues).foreach { case (idx, value) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseLongKeySortedFloatVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TLongFloatVector]
          x.getIndexes.zip(x.getValues).foreach { case (idx, value) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: DenseDoubleVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          x.getValues.zipWithIndex.foreach { case (value, idx) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: DenseFloatVector =>
        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          x.getValues.zipWithIndex.foreach { case (value, idx) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          fxValue += 0.5 * (ret1 * ret1 - ret2)
        }
      case _ => throw new Exception("Data type not support!")
    }

    fxValue
  }

  private def updateGrad(x: TVector, params: util.HashMap[String, TUpdate], gradient: util.HashMap[String, TUpdate], derviationMultipler: Double): Unit = {
    val vMatrix = params.get(vmat).asInstanceOf[RowbaseMatrix[_]]
    val gmat = gradient.get(vmat).asInstanceOf[RowbaseMatrix[_]]

    x match {
      case x: SparseDummyVector =>
        for (f <- 0 until rank) {
          vMatrix.getRow(f) match {
            case v_row: TDoubleVector =>
              val dot = v_row.dot(x)
              val gv = gmat.getRow(f).asInstanceOf[TDoubleVector]
              x.getIndices.foreach { idx =>
                val g = derviationMultipler * (dot - v_row.get(idx))
                gv.plusBy(idx, g)
              }
            case v_row: TFloatVector =>
              val dot = v_row.dot(x)
              val gv = gmat.getRow(f).asInstanceOf[TFloatVector]
              x.getIndices.foreach { idx =>
                val g = derviationMultipler * (dot - v_row.get(idx))
                gv.plusBy(idx, g.toFloat)
              }
          }
        }
      case x: SparseLongKeyDummyVector =>
        for (f <- 0 until rank) {
          vMatrix.getRow(f) match {
            case v_row: TLongDoubleVector =>
              val dot = v_row.dot(x)
              val gv = gmat.getRow(f).asInstanceOf[TLongDoubleVector]
              x.getIndices.foreach { idx =>
                val g = derviationMultipler * (dot - v_row.get(idx))
                gv.plusBy(idx, g)
              }
            case v_row: TLongFloatVector =>
              val dot = v_row.dot(x)
              val gv = gmat.getRow(f).asInstanceOf[TLongFloatVector]
              x.getIndices.foreach { idx =>
                val g = derviationMultipler * (dot - v_row.get(idx))
                gv.plusBy(idx, g.toFloat)
              }
          }
        }
      case x: SparseDoubleVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          val gv = gmat.getRow(f).asInstanceOf[TDoubleVector]
          val dot = v_row.dot(x)
          val iter = x.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val idx = entry.getIntKey
            val value = entry.getDoubleValue
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g)
          }
        }
      case x: SparseFloatVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          val gv = gmat.getRow(f).asInstanceOf[TFloatVector]
          val dot = v_row.dot(x)
          val iter = x.getIndexToValueMap.int2FloatEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val idx = entry.getIntKey
            val value = entry.getFloatValue
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g.toFloat)
          }
        }
      case x: SparseDoubleSortedVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          val gv = gmat.getRow(f).asInstanceOf[TDoubleVector]
          val dot = v_row.dot(x)
          x.getIndices.zip(x.getValues).foreach { case (idx, value) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g)
          }
        }
      case x: SparseFloatSortedVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          val gv = gmat.getRow(f).asInstanceOf[TFloatVector]
          val dot = v_row.dot(x)
          x.getIndices.zip(x.getValues).foreach { case (idx, value) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g.toFloat)
          }
        }
      case x: SparseLongKeySortedDoubleVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TLongDoubleVector]
          val gv = gmat.getRow(f).asInstanceOf[TLongDoubleVector]
          val dot = v_row.dot(x)
          x.getIndexes.zip(x.getValues).foreach { case (idx, value) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g)
          }
        }
      case x: SparseLongKeySortedFloatVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TLongFloatVector]
          val gv = gmat.getRow(f).asInstanceOf[TLongFloatVector]
          val dot = v_row.dot(x)
          x.getIndexes.zip(x.getValues).foreach { case (idx, value) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g.toFloat)
          }
        }
      case x: DenseDoubleVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TDoubleVector]
          val gv = gmat.getRow(f).asInstanceOf[TDoubleVector]
          val dot = v_row.dot(x)
          x.getValues.zipWithIndex.foreach { case (value, idx) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g)
          }
        }
      case x: DenseFloatVector =>
        for (f <- 0 until rank) {
          val v_row = vMatrix.getRow(f).asInstanceOf[TFloatVector]
          val gv = gmat.getRow(f).asInstanceOf[TFloatVector]
          val dot = v_row.dot(x)
          x.getValues.zipWithIndex.foreach { case (value, idx) =>
            val g = derviationMultipler * (dot - value * v_row.get(idx)) * value
            gv.plusBy(idx, g.toFloat)
          }
        }
      case _ => throw new Exception("Data type not support!")
    }

    gradient.get(weight).asInstanceOf[TVector].plusBy(x, derviationMultipler)
    gradient.get(intercept).asInstanceOf[TVector].plusBy(0, derviationMultipler)
  }

  override def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val start = System.currentTimeMillis()

    val params = modelType match {
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_FLOAT_SPARSE_LONGKEY =>
        pullParamsFromPS(Array[Long](), getIndexFlag)
      case _ => pullParamsFromPS(Array[Int](), getIndexFlag)
    }

    val vMatrix = params.get(vmat).asInstanceOf[RowbaseMatrix[_]]
    val wVector = params.get(weight).asInstanceOf[TVector]
    val bias = getBias(params.get(intercept))

    val cost = System.currentTimeMillis() - start
    LOG.info(s"pull LR Model from PS cost $cost ms.")

    val predict = new MemoryDataBlock[PredictResult](-1)

    dataSet.resetReadIndex()
    for (_: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val fxValue = calModel(instance.getX, vMatrix, wVector, bias)
      val sig = Maths.sigmoid(fxValue)
      predict.put(new LRPredictResult(instance.getY, fxValue, sig))
    }

    predict
  }

  override def calLossAndUpdateGrad(x: TVector, y: Double,
                                    params: util.HashMap[String, TUpdate],
                                    gradient: util.HashMap[String, TUpdate]): Double = {
    val vMatrix = params.get(vmat).asInstanceOf[RowbaseMatrix[_]]
    val wVector = params.get(weight).asInstanceOf[TVector]
    val bias = getBias(params.get(intercept))

    val fxValue = calModel(x, vMatrix, wVector, bias)

    // - y / (1.0 + Math.exp(y * fxValue))
    val derviationMultipler = LogisticLoss.grad(fxValue, y)

    updateGrad(x, params, gradient, derviationMultipler)

    LogisticLoss(fxValue, y)
  }

  override def calPredAndLoss(x: TVector, y: Double, params: util.HashMap[String, TUpdate]): (Double, Double) = {
    val vMatrix = params.get(vmat).asInstanceOf[RowbaseMatrix[_]]
    val wVector = params.get(weight).asInstanceOf[TVector]
    val bias = getBias(params.get(intercept))

    val fxValue = calModel(x, vMatrix, wVector, bias)

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
    flag.put(vmat, true)
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
