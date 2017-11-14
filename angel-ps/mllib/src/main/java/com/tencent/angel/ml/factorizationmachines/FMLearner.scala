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
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleSortedVector}
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
  * @param ctx: context of this task
  * @param minP: min value of y
  * @param maxP: max value of y
  * @param feaUsed: array of used feature of the input data
  */
class FMLearner(override val ctx: TaskContext, val minP: Double, val maxP: Double, val feaUsed: Array[Int]) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[FMLearner])
  val fmmodel = new FMModel(conf, ctx)

  val learnType = conf.get(MLConf.ML_FM_LEARN_TYPE, MLConf.DEFAULT_ML_FM_LEARN_TYPE)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)

  val rank: Int = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)
  val reg0: Double = conf.getDouble(MLConf.ML_FM_REG0, MLConf.DEFAULT_ML_FM_REG0)
  val reg1: Double = conf.getDouble(MLConf.ML_FM_REG1, MLConf.DEFAULT_ML_FM_REG1)
  val reg2: Double = conf.getDouble(MLConf.ML_FM_REG2, MLConf.DEFAULT_ML_FM_REG2)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val vStddev: Double = conf.getDouble(MLConf.ML_FM_V_STDDEV, MLConf.DEFAULT_ML_FM_V_INIT)
  // Put used feature indexes to vIndexs
  val vIndexs = new RowIndex()
  feaUsed.zipWithIndex.filter((p:(Int, Int))=>p._1!=0).map((p:(Int, Int))=>vIndexs.addRowId(p._2))
  val feaUsedN = vIndexs.getRowsNumber
  LOG.info("vIndexs's row's number = " + vIndexs)

  /**
    * Train a Factorization machines Model
    *
    * @param trainData : input train data storage
    * @param vali      : validate data storage
    * @return : a learned model
    */
  override
  def train(trainData: DataBlock[LabeledData], vali: DataBlock[LabeledData]):
  MLModel = {
    val start = System.currentTimeMillis()
    LOG.info(s"learnType=$learnType, feaNum=$feaNum, rank=$rank, #trainData=${trainData.size}")
    LOG.info(s"reg0=$reg0, reg1=$reg1, reg2=$reg2, lr=$lr, vStev=$vStddev")

    val beforeInit = System.currentTimeMillis()
    initModels()
    val initCost = System.currentTimeMillis() - beforeInit
    LOG.info(s"Init matrixes cost $initCost ms.")

    globalMetrics.addMetric(fmmodel.FM_OBJ, LossMetric(trainData.size()))

    while (ctx.getEpoch < epochNum) {
      val startIter = System.currentTimeMillis()
      val (w0, w, v) = oneIteration(trainData)
      val iterCost = System.currentTimeMillis() - startIter

      val startVali = System.currentTimeMillis()
      val loss = evaluate(trainData, w0.get(0), w, v)
      val valiCost = System.currentTimeMillis() - startVali

      globalMetrics.metric(fmmodel.FM_OBJ, loss)

      LOG.info(s"Epoch=${ctx.getEpoch}, evaluate loss=${loss/trainData.size()}. " +
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
    if(ctx.getTaskId.getIndex == 0) {
      for (row <- 0 until feaNum) {
        fmmodel.v.update(new RandomNormal(fmmodel.v.getMatrixId(), row, 0.0, vStddev)).get()
      }
    }

    fmmodel.v.syncClock()
  }

  /**
    * One iteration to train Factorization Machines
 *
    * @param dataBlock
    * @return
    */
  def oneIteration(dataBlock: DataBlock[LabeledData]): (DenseDoubleVector, DenseDoubleVector, mutable.HashMap[Int, DenseDoubleVector]) = {
    val startGet = System.currentTimeMillis()
    val (w0, w, v) = fmmodel.pullFromPS(vIndexs)
    val getCost = System.currentTimeMillis() - startGet
    LOG.info(s"Get matrixes cost $getCost ms.")

    val _w0 = w0.clone().asInstanceOf[DenseDoubleVector]
    val _w = w.clone().asInstanceOf[DenseDoubleVector]
    val _v = new mutable.HashMap[Int, DenseDoubleVector]()
    for (vec <- v) {
      _v.put(vec._1, vec._2.clone().asInstanceOf[DenseDoubleVector])
    }

    dataBlock.resetReadIndex()
    for (_ <- 0 until dataBlock.size) {
      val data = dataBlock.read()
      val x = data.getX.asInstanceOf[SparseDoubleSortedVector]
      val y = data.getY
      val pre = predict(x, y, _w0.get(0), _w, _v)
      val dm = derviationMultipler(y, pre)

      _w0.plusBy(0, -lr * (dm + reg0 * _w0.get(0)))
      _w.timesBy(1 - lr * reg1).plusBy(x, -lr * dm)
      updateV(x, dm, _v)
    }

    for (update <- _v) {
      v(update._1).plusBy(update._2, -1.0).timesBy(-1.0)
    }

    fmmodel.pushToPS(w0.plusBy(_w0, -1.0).timesBy(-1.0).asInstanceOf[DenseDoubleVector],
      w.plusBy(_w, -1.0).timesBy(-1.0).asInstanceOf[DenseDoubleVector],
      v)

    (_w0, _w, _v)
  }

  /**
    * Evaluate the objective value
    * For regression: loss(y,\hat y) = (y - \hat y)^2
    * For classification: loss(y,\hat y) = -\ln (\delta(y, \hat y))),
    *                     in which \delta(x) = \frac{1}{1+e^{-x}}
 *
    * @param dataBlock
    * @param w0
    * @param w
    * @param v
    * @return
    */
  def evaluate(dataBlock: DataBlock[LabeledData], w0: Double, w: DenseDoubleVector,
               v: mutable.HashMap[Int, DenseDoubleVector]):
  Double = {
    dataBlock.resetReadIndex()

    learnType match {
      case "r" => {
        var eval = 0.0
        for (_ <- 0 until dataBlock.size) {
          val data = dataBlock.read()
          val x = data.getX.asInstanceOf[SparseDoubleSortedVector]
          val y = data.getY
          val pre = predict(x, y, w0, w, v)
          eval += (pre - y) * (pre - y)
        }
        LOG.info(s"Regression evaluate loss=$eval")

        eval
      }

      case "c" => {
        var loss = 0.0
        var correct = 0.0
        var tp = 0
        var fp = 0
        var tn = 0
        var fn = 0
        val yList = new Array[Double](dataBlock.size())
        val preList = new Array[Double](dataBlock.size())

        for (i <- 0 until dataBlock.size()) {
          val data = dataBlock.read()
          val x = data.getX.asInstanceOf[SparseDoubleSortedVector]
          val y = data.getY
          val pre = predict(x, y, w0, w, v)

          yList(i) = y
          preList(i) = pre

          pre * y match {
            case dot if dot > 0 => {
              correct += 1

              y match {
                case y if y > 0 => tp += 1
                case y if y < 0 => tn += 1
              }
            }

            case dot if dot < 0 => {
              y match {
                case y if y > 0 => fn += 1
                case y if y < 0 => fp += 1
              }
            }

              loss += Math.log1p(1 / (1 + Math.exp(-dot)))
          }

        }
        val metric = ValidationUtils.calAUC(preList, yList, tp, tn, fp, fn)
        LOG.info(s"loss=${loss/yList.length}, precision=${correct/yList.length},auc=${metric._1}, " +
          s"trueRecall=${metric._2}, falseRecall=${metric._3}")

        loss
      }

    }
  }

  /**
    * Predict an instance
 *
    * @param x：feature vector of instance
    * @param y: label value of instance
    * @param w0: w0 mat of FM
    * @param w: w mat of FM
    * @param v: v mat of FM
    * @return
    */
  def predict(x: SparseDoubleSortedVector,
              y: Double, w0: Double,
              w: DenseDoubleVector,
              v: mutable.HashMap[Int, DenseDoubleVector]): Double = {

    var ret: Double = 0.0
    ret += w0
    ret += x.dot(w)

    for (f <- 0 until rank) {
      var ret1 = 0.0
      var ret2 = 0.0
      for (i <- 0 until x.size()) {
        val tmp = x.getValues()(i) * v(x.getIndices()(i)).get(f)
        ret1 += tmp
        ret2 += tmp * tmp
      }
      ret += 0.5 * (ret1 * ret1 - ret2)
    }

    learnType match {
      // For classification:
      case "r" => ret = if (ret < maxP) ret else maxP
                  ret = if (ret > minP) ret else minP
      // For regression:
      case "c" => 1.0 / (1.0 + Math.exp(-ret))
    }

    ret
  }

  /**
    * \frac{\partial loss}{\partial x} = dm * \frac{\partial y}{\partial x}
 *
    * @param y: label of the instance
    * @param pre: predict value of the instance
    * @return : dm value
    */
  def derviationMultipler(y: Double, pre: Double): Double = {
    learnType match {
      // For classification:
      // loss=-ln(\delta (pre\cdot y))
      // \frac{\partial loss}{\partial x}=(\delta (pre\cdot y)-1)y\frac{\partial y}{\partial x}
      case "c" => -y * (1.0 - 1.0 / (1 + Math.exp(-y * pre)))
      // For regression:
      // loss = (pre-y)^2
      // \frac{\partial loss}{\partial x}=2(pre-y)\frac{\partial y}{\partial x}
      //      case "r" => 2 * (pre - y)
      case "r" => pre - y
    }
  }

  /**
    * Update v mat
 *
    * @param x: a train instance
    * @param dm: dm value of the instance
    * @param v: v mat
    */
  def updateV(x: SparseDoubleSortedVector, dm: Double, v: mutable.HashMap[Int, DenseDoubleVector]):
  Unit = {

    for (f <- 0 until rank) {
      // calculate dot(vf, x)
      var dot = 0.0
      for (i <- 0 until x.size()) {
        dot += x.getValues()(i) * v(x.getIndices()(i)).get(f)
      }

      for (i <- 0 until x.size()) {
        val j = x.getIndices()(i)
        val grad = dot * x.getValues()(i) - v(j).get(f) * x.getValues()(i) * x.getValues()(i)
        v(j).plusBy(f, -lr * (dm * grad + reg2 * v(j).get(f)))
      }
    }
  }

}
