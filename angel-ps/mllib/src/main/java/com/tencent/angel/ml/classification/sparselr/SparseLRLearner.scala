/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.classification.sparselr

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.optimizer.admm.ADMM
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

class SparseLRLearner(ctx: TaskContext) extends MLLearner(ctx) {

  private val LOG = LogFactory.getLog(classOf[SparseLRLearner])

  private var regParam = 0.0
  private var rho = 0.01
  private var maxIter = 15
  private var threadNum = 4


  /**
    * Set the regularization parameter. Default 0.0
    */
  def setRegParam(reg: Double): this.type = {
    regParam = reg
    this
  }

  def setMaxIter(num: Int): this.type = {
    maxIter = num
    this
  }

  def setRho(factor: Double): this.type = {
    rho = factor
    this
  }

  def setThreadNum(num: Int): this.type = {
    threadNum = num
    this
  }

  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): SparseLRModel = {
    val model = new SparseLRModel(conf, ctx)

    LOG.info(s"threadNum=$threadNum maxIter=$maxIter")

    globalMetrics.addMetric(TRAIN_LOSS, LossMetric(train.size()))

    val (history, z) = ADMM.runADMM(train, model)(
      regParam, rho, ctx.getTotalTaskNum, threadNum, maxIter)(
      ctx, globalMetrics)

    if (ctx.getTaskIndex == 0) {
      model.z.increment(0, z)
      model.z.clock().get()
    } else {
      model.z.clock(false).get()
    }
    model
  }

}
