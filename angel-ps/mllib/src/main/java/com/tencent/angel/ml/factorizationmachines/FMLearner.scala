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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.factorizationmachines

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleSortedVector, SparseDoubleVector, SparseDummyVector}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.utils.ValidationUtils
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable

/**
  * Learner of Factorization machines
  *
  * @param ctx  : context of this task
  * @param minP : min value of y
  * @param maxP : max value of y
  */
class FMLearner(override val ctx: TaskContext, val minP: Double, val maxP: Double) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[FMLearner])
  val fmmodel = new FMModel(conf, ctx)

  val learnType: String = conf.get(MLConf.ML_FM_LEARN_TYPE, MLConf.DEFAULT_ML_FM_LEARN_TYPE)
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)

  val rank: Int = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)
  val reg1: Double = conf.getDouble(MLConf.ML_FM_REG_L2_W, MLConf.DEFAULT_ML_FM_REG_L2_W)
  val reg2: Double = conf.getDouble(MLConf.ML_FM_REG_L2_V, MLConf.DEFAULT_ML_FM_REG_L2_V)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val vStddev: Double = conf.getDouble(MLConf.ML_FM_V_STDDEV, MLConf.DEFAULT_ML_FM_V_INIT)
  val miniBatcSize: Int = conf.getInt(MLConf.ML_MINIBATCH_SIZE, MLConf.DEFAULT_ML_MINIBATCH_SIZE)
  val vIndexs = new RowIndex()
  (0 until rank).foreach(vIndexs.addRowId)

  /**
    * Train a Factorization machines Model
    *
    * @param trainData : input train data storage
    * @param vali      : validate data storage
    * @return : a learned model
    */
  override
  def train(trainData: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = {
    val start = System.currentTimeMillis()
    LOG.info(s"learnType=$learnType, feaNum=$indexRange, rank=$rank, #trainData=${trainData.size}")
    LOG.info(s"reg1=$reg1, reg2=$reg2, lr=$lr, vStev=$vStddev")

    val beforeInit = System.currentTimeMillis()
    initModels()
    val initCost = System.currentTimeMillis() - beforeInit
    LOG.info(s"Init matrixes cost $initCost ms.")

    globalMetrics.addMetric(fmmodel.FM_OBJ, LossMetric(trainData.size()))

    while (ctx.getEpoch < epochNum) {
      val startIter = System.currentTimeMillis()
      LOG.info(s"The ${ctx.getEpoch} Epoch in Task ${ctx.getTaskId.toString} started!")
      val (w0, w, v) = oneIteration(trainData)
      LOG.info(s"The ${ctx.getEpoch} Epoch in Task ${ctx.getTaskId.toString} finished!")
      val iterCost = System.currentTimeMillis() - startIter

      val startVali = System.currentTimeMillis()
      val loss = evaluate(trainData, w0, w, v)
      val valiCost = System.currentTimeMillis() - startVali

      globalMetrics.metric(fmmodel.FM_OBJ, loss)

      LOG.info(s"Epoch=${ctx.getEpoch}, evaluate loss=${loss / trainData.size()}. " +
        s"trainCost=$iterCost, " +
        s"valiCost=$valiCost")

      ctx.incEpoch()
    }

    val end = System.currentTimeMillis()
    val cost = end - start
    LOG.info(s"FM Learner train cost $cost ms.")

    fmmodel
  }

  /**
    * Initialize with random values
    */
  def initModels(): Unit = {
    if (ctx.getTaskId.getIndex == 0) {
      LOG.info(s"${ctx.getTaskId} is in charge of intial model, start ...")
      fmmodel.w0.zero()
      LOG.info(s"w0 initial finished!")
      fmmodel.w.update(new RandomNormal(fmmodel.w.getMatrixId(), 0, 0.0, vStddev)).get()
      LOG.info(s"w initial finished!")
      fmmodel.v.update(new RandomNormal(fmmodel.v.getMatrixId(), 0, rank, 0.0, vStddev)).get()

      LOG.info(s"v initial finished!")
      LOG.info(s"${ctx.getTaskId} finished intial model!")
    }

    LOG.info(s"Now begin to syncClock w0, w, v in the frist time!")
    fmmodel.w0.syncClock()
    fmmodel.w.syncClock()
    fmmodel.v.syncClock()
  }

  /**
    * One iteration to train Factorization Machines
    *
    * @param dataBlock
    * @return
    */
  def oneIteration(dataBlock: DataBlock[LabeledData]): (DenseDoubleVector, DenseDoubleVector, mutable.Map[Int, TVector]) = {
    val startGet = System.currentTimeMillis()
    val (w0, w, v) = fmmodel.pullFromPS(vIndexs)
    val getCost = System.currentTimeMillis() - startGet
    LOG.info(s"Get matrixes cost $getCost ms.")

    val _w0 = w0.clone()
    val _w = w.clone()
    val _v: mutable.Map[Int, TVector] = new mutable.HashMap[Int, TVector]()
    v.foreach { case (idx, vec) => _v.put(idx, vec.clone()) }

    dataBlock.resetReadIndex()
    LOG.info(s"Start trainning in oneIteration ...")

    // initial gredient
    var grad_w0: Double = 0.0
    val capacity = Math.max(w.getDimension / 100, 64)
    val grad_w1: SparseDoubleVector = new SparseDoubleVector(w.getDimension, capacity)
    val grad_v: mutable.Map[Int, SparseDoubleVector] = new mutable.HashMap[Int, SparseDoubleVector]()
    v.foreach { case (idx, _) => grad_v.put(idx, new SparseDoubleVector(w.getDimension, capacity)) }

    var batchInnerCounter = 0
    for (_ <- 0 until dataBlock.size) {
      val data = dataBlock.read()
      val (x, y) = (data.getX, data.getY)
      val pre = fmmodel.calModel(x, _w0, _w, _v)
      val dm = derviationMultipler(y, pre)
      batchInnerCounter += 1

      // calculate gradient
      grad_w0 += dm
      grad_w1.plusBy(x, dm)
      _v.foreach { case (idx, v_row_) =>
        val v_row = v_row_.asInstanceOf[DenseDoubleVector]
        val dot = v_row.dot(x)

        x match {
          case ifeat: SparseDoubleSortedVector =>
            ifeat.getValues.zip(ifeat.getIndices).foreach { case (value, i) =>
              grad_v(idx).plusBy(i, dm * (dot - v_row.get(i) * value) * value)
            }
          case ifeat: SparseDummyVector =>
            ifeat.getIndices.foreach { i =>
              grad_v(idx).plusBy(i, dm * (dot - v_row.get(i)))
            }
          case ifeat: DenseDoubleVector =>
            ifeat.getValues.zipWithIndex.foreach { case (value, i) =>
              grad_v(idx).plusBy(i, dm * (dot - v_row.get(i) * value) * value)
            }
          case _ => throw new Exception("Data type not support!")
        }
      }

      if (batchInnerCounter == miniBatcSize) {
        // 1. update parameter
        updateParameters(_w0, _w, _v, grad_w0, grad_w1, grad_v, miniBatcSize)

        // 2. reset count and grad
        batchInnerCounter = 0
        grad_w0 = 0.0
        grad_w1.clear()
        grad_v.foreach { case (_, vect) => vect.clear() }
      }
    }

    if (batchInnerCounter != 0) {
      updateParameters(_w0, _w, _v, grad_w0, grad_w1, grad_v, batchInnerCounter)
    }

    LOG.info(s"Calculate Delta for update ...")
    calDeltaInplace(w0, w, v, _w0, _w, _v)

    LOG.info(s"Begin to update parameter in PS ...")
    fmmodel.pushToPS(w0, w, v)
    LOG.info(s"Parameter has update in PS ...")

    (_w0, _w, _v)
  }

  /**
    * @param dataBlock
    * @param w0
    * @param w
    * @param v
    * @return
    */
  def evaluate(dataBlock: DataBlock[LabeledData], w0: DenseDoubleVector, w: DenseDoubleVector,
               v: mutable.Map[Int, TVector]): Double = {
    dataBlock.resetReadIndex()

    var loss = 0.0
    learnType match {
      case "r" =>
        for (_ <- 0 until dataBlock.size) {
          val data = dataBlock.read()
          val (x, y) = (data.getX, data.getY)
          val pre = fmmodel.calModel(x, w0, w, v)
          loss += 0.5 * (pre - y) * (pre - y)
        }
        LOG.info(s"Regression evaluate loss=$loss")

      case "c" =>
        var correct = 0.0
        var (tp, fp, tn, fn) = (0, 0, 0, 0)
        val yList = new Array[Double](dataBlock.size())
        val preList = new Array[Double](dataBlock.size())

        for (i <- 0 until dataBlock.size()) {
          val data = dataBlock.read()
          val (x, y) = (data.getX, data.getY)
          val pre = fmmodel.calModel(x, w0, w, v)

          yList(i) = y
          preList(i) = 1.0 / (1.0 + Math.exp(-pre))

          if ((pre > 0.0 && y > 0) || (pre < 0.0 && y <= 0)) {
            correct += 1
            if (pre > 0.0) {
              tp += 1
            } else {
              tn += 1
            }
          } else {
            if (pre > 0.0) {
              fp += 1
            } else {
              fn += 1
            }
          }

          val weight = if (y > 0) {
            conf.getDouble(MLConf.ML_POSITIVE_SAMPLE_WEIGHT, 1.0)
          } else {
            conf.getDouble(MLConf.ML_NEGATIVE_SAMPLE_WEIGHT, 1.0)
          }
          loss += weight * Math.log1p(Math.exp(-y * pre))
        }

        val metric = ValidationUtils.calAUC(preList, yList, tp, tn, fp, fn)
        LOG.info(s"loss=${loss / yList.length}, precision=${correct / yList.length},auc=${metric._1}, " +
          s"trueRecall=${metric._2}, falseRecall=${metric._3}")
    }

    loss / dataBlock.size
  }

  /**
    * @param y   : label of the instance
    * @param pre : predict value of the instance
    * @return : dm value
    */
  def derviationMultipler(y: Double, pre: Double): Double = {
    learnType match {
      case "c" =>
        val weight = if (y > 0) {
          conf.getDouble(MLConf.ML_POSITIVE_SAMPLE_WEIGHT, 1.0)
        } else {
          conf.getDouble(MLConf.ML_NEGATIVE_SAMPLE_WEIGHT, 1.0)
        }
        -weight * y / (1.0 + Math.exp(y * pre))
      case "r" =>
        pre - y
    }
  }

  def calDeltaInplace(w0_old: DenseDoubleVector, w_old: DenseDoubleVector, v_old: mutable.Map[Int, TVector],
                      w0_new: DenseDoubleVector, w_new: DenseDoubleVector, v_new: mutable.Map[Int, TVector]
                     ): Unit = {
    w0_old.plusBy(w0_new, -1.0).timesBy(-1.0)
    w_old.plusBy(w_new, -1.0).timesBy(-1.0)
    v_new.foreach { case (idx, vec) =>
      v_old(idx).asInstanceOf[DenseDoubleVector].plusBy(vec.asInstanceOf[DenseDoubleVector], -1.0).timesBy(-1.0)
    }
  }

  def updateParameters(w0_param: DenseDoubleVector, w_param: DenseDoubleVector, v_param: mutable.Map[Int, TVector],
                       w0_grad: Double, w_grad: SparseDoubleVector, v_grad: mutable.Map[Int, SparseDoubleVector],
                       numSamples: Int): Unit = {
    w0_param.plusBy(0, -lr * w0_grad / numSamples)
    w_param.timesBy(1.0 - lr * reg1).plusBy(w_grad, -lr / numSamples)
    v_param.foreach { case (i, vect) =>
      vect.timesBy(1.0 - lr * reg2).plusBy(v_grad(i), -lr / numSamples)
    }
  }

}
