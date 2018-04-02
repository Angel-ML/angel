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

package com.tencent.angel.example.quickStart

import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.optimizer.sgd.loss.L2LogLoss
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * A quick start Logistic Regression train task
 *
  * @param ctx taskContext of task
  */
class QSLRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx){
  val LOG: Log = LogFactory.getLog(classOf[QSLRTrainTask])

  val epochNum: Int = conf.getInt(ML_EPOCH_NUM, DEFAULT_ML_EPOCH_NUM)
  val lr: Double = conf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEAR_RATE)
  val feaNum: Int = conf.getInt(ML_FEATURE_INDEX_RANGE, DEFAULT_ML_FEATURE_INDEX_RANGE)
  val dataFormat: String = conf.get(ML_DATA_INPUT_FORMAT, DEFAULT_ML_DATA_FORMAT)
  val reg: Double = conf.getDouble(ML_LR_REG_L2, DEFAULT_ML_LR_REG_L2)
  val loss = new L2LogLoss(reg)

  /**
    * Parse input text as labeled data, X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  /**
    * Train a LR model iteratively
 *
    * @param ctx context of this Task
    */
  override
  def train(ctx: TaskContext): Unit = {
    // A simple logistic regression model
    val model = new QSLRModel(conf, ctx)

    // Apply batch gradient descent LR iteratively
    while (ctx.getEpoch < epochNum) {
      // Pull model from PS
      val weight = model.weight.getRow(0).asInstanceOf[TDoubleVector]

      // Calculate gradient vector
      val grad = batchGradientDescent(weight)

      // Push gradient vector to PS Server and synchronize
      model.weight.increment(grad)
      model.weight.syncClock()

      // Increase epoch number
      ctx.incEpoch()
    }
  }

  /**
    * Iterating samples, calculates the loss and summate the grad
    *
    * @param weight LR model vector
    * @return gradient vector
    */
  def batchGradientDescent(weight: TDoubleVector): TDoubleVector = {
    var (grad, batchLoss) = (new DenseDoubleVector(feaNum), 0.0)

    taskDataBlock.resetReadIndex()
    for (i <- 0 until taskDataBlock.size) {
      val data = taskDataBlock.read()
      val pred = weight.dot(data.getX)
      grad.plusBy(data.getX, -1.0 * loss.grad(pred, data.getY))
      batchLoss += loss.loss(pred, data.getY)
    }
    grad.timesBy(-lr / taskDataBlock.size)

    LOG.info(s"Gradient descent batch ${ctx.getEpoch}, batch loss=$batchLoss")
    grad.setRowId(0)
    grad
  }

}
