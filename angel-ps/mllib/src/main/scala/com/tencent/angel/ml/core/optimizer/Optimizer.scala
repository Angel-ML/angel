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


package com.tencent.angel.ml.core.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import org.json4s.JsonAST.JObject

abstract class Optimizer(stepSize: Double) extends Serializable {
  protected var numSlot: Int
  protected var lr: Double = stepSize
  protected var epsilon: Double = 1e-7
  protected var regL1Param: Double = SharedConf.get().getDouble(MLConf.ML_REG_L1)
  protected var regL2Param: Double = SharedConf.get().getDouble(MLConf.ML_REG_L2)

  def setRegL1Param(regParam: Double): this.type = {
    this.regL1Param = regParam
    this
  }

  def getRegL1Param: Double = this.regL1Param

  def setRegL2Param(regParam: Double): this.type = {
    this.regL2Param = regParam
    this
  }

  def getRegL2Param: Double = this.regL2Param

  def getLR: Double = this.lr

  def setLR(lr: Double): Unit = {
    this.lr = lr
  }

  def setNumSlot(numSlot: Int): Unit = {
    this.numSlot = numSlot
  }

  def getNumSlot: Int = this.numSlot

  def setEpsilon(epsilon: Double): Unit = {
    this.epsilon = epsilon
  }

  def getEpsilon: Double = this.epsilon

  def update(matrixId: Int, numFactors: Int, epoch: Int): Future[VoidResult]

  def update(matrixId: Int, numFactors: Int, epoch: Int, batchSize: Int): Future[VoidResult]

  def toJson: JObject
}
