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

import java.text.DecimalFormat

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleSortedVector, SparseDummyVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.{DataBlock, MemoryAndDiskDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

/*
 * Factorization machines model
 */
object FMModel {
  def apply(conf: Configuration) = {
    new FMModel(conf)
  }

  def apply(conf: Configuration, ctx: TaskContext) = {
    new FMModel(conf, ctx)
  }
}


class FMModel(conf: Configuration = null, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[FMModel])

  val FM_W0 = "fm_w0"
  val FM_W = "fm_w"
  val FM_V = "fm_v"
  val FM_OBJ = "fm_evaluate"

  // Feature number of data
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)
  // Rank of each feature vector
  val rank = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)
  // val rowType =  RowType.valueOf(conf.get(MLConf.LR_MODEL_TYPE, RowType.T_DOUBLE_SPARSE.toString))

  // The w0 weight vector, stored on PS # setOplogType("SPARSE_DOUBLE")
  val w0 = PSModel(FM_W0, 1, 1).setAverage(true)
  // The w weight vector, stored on PS
  val w = PSModel(FM_W, 1, indexRange, -1, -1, modelSize).setAverage(true)
  // The v weight vector, stored on PS
  private val blockCol = if (rank * indexRange < 1000000) -1 else 1000000 / rank
  val v = PSModel(FM_V, rank, indexRange, rank, blockCol, modelSize).setAverage(true)

  addPSModel(w0)
  addPSModel(w)
  addPSModel(v)

  super.setSavePath(conf)
  super.setLoadPath(conf)

  def calModel(xvec: TVector, w0: DenseDoubleVector, w: DenseDoubleVector, v: mutable.Map[Int, TVector]): Double = {
    // val y = data.getY
    var ret: Double = 0

    xvec match {
      case x: SparseDoubleSortedVector =>
        ret = w0.get(0) + w.dot(x)

        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = v(f).asInstanceOf[DenseDoubleVector]
          x.getIndices.zip(x.getValues).foreach { case (idx, value) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          ret += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: SparseDummyVector =>
        ret = w0.get(0) + w.dot(x)

        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = v(f).asInstanceOf[DenseDoubleVector]
          x.getIndices.foreach { idx =>
            val tmp = v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          ret += 0.5 * (ret1 * ret1 - ret2)
        }
      case x: DenseDoubleVector =>
        ret = w0.get(0) + w.dot(x)

        for (f <- 0 until rank) {
          var (ret1, ret2) = (0.0, 0.0)
          val v_row = v(f).asInstanceOf[DenseDoubleVector]
          x.getValues.zipWithIndex.foreach { case (value, idx) =>
            val tmp = value * v_row.get(idx)
            ret1 += tmp
            ret2 += tmp * tmp
          }
          ret += 0.5 * (ret1 * ret1 - ret2)
        }
      case _ => throw new Exception("Data type not support!")
    }

    ret = if (ret < Double.MaxValue) ret else Double.MaxValue
    ret = if (ret > Double.MinValue) ret else Double.MinValue

    ret
  }

  /**
    *
    * @param dataSet
    * @return
    */
  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val fmmodel = new FMModel(conf, ctx)
    val vIndexs = new RowIndex()
    (0 until rank).foreach(vIndexs.addRowId)
    val (_w0, _w, _v) = fmmodel.pullFromPS(vIndexs)

    val result = new MemoryAndDiskDataBlock[PredictResult](ctx.getTaskIndex)

    for (sid <- 0 until dataSet.size) {
      val data = dataSet.read()
      val ret = calModel(data.getX, _w0, _w, _v)
      val learnType = conf.get(MLConf.ML_FM_LEARN_TYPE, MLConf.DEFAULT_ML_FM_LEARN_TYPE)

      learnType match {
        case "c" =>
          result.put(FMPredictResult(sid, Maths.sigmoid(ret)))
        case "r" =>
          result.put(FMPredictResult(sid, ret))
      }

    }

    result
  }

  /** Pull w0, w, v from PS
    *
    * @param vIndexs
    * @return
    */
  def pullFromPS(vIndexs: RowIndex) = {
    LOG.info("Start to pull w0 from PS ...")
    val w0_pulled = w0.getRow(0).asInstanceOf[DenseDoubleVector]
    LOG.info("w0 is pulled !")

    LOG.info("Start to pull w from PS ...")
    val w_pulled = w.getRow(0).asInstanceOf[DenseDoubleVector]
    LOG.info("w is pulled !")

    LOG.info("Start to pull v from PS ...")
    val v_pulled = v.getRows(vIndexs, -1)
    LOG.info("v is pulled !")
    (w0_pulled, w_pulled, v_pulled)
  }

  /** Push update values of w0, w, v
    *
    * @param update0 : update values of w0
    * @param update1 : update values of w
    * @param update2 : update values of v
    * @return
    */
  def pushToPS(update0: DenseDoubleVector,
               update1: DenseDoubleVector,
               update2: mutable.Map[Int, TVector]) = {
    w0.increment(0, update0)
    w.increment(0, update1)
    update2.foreach { case (idx, vec) => v.increment(idx, vec) }

    LOG.info("Start to push w0 from PS ...")
    w0.syncClock()
    LOG.info("w0 is pushed !")

    LOG.info("Start to push w from PS ...")
    w.syncClock()
    LOG.info("w is pushed !")

    LOG.info("Start to push v from PS ...")
    v.syncClock()
    LOG.info("v is pushed !")
  }
}


case class FMPredictResult(id: Double, pval: Double) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText = {
    df.format(id) + separator + format.format(pval)
  }
}

