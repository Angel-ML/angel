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

import com.tencent.angel.ml.core.network.variable.Updater
import org.json4s.JsonAST.{JObject, JValue}



trait Optimizer extends Updater with Serializable {
  var lr: Double

  protected var regL1Param: Double
  protected var regL2Param: Double

  def setLR(lr: Double): this.type = {
    this.lr = lr
    this
  }

  def setRegL1Param(regParam: Double): this.type = {
    this.regL1Param = regParam
    this
  }

  def setRegL2Param(regParam: Double): this.type = {
    this.regL2Param = regParam
    this
  }

  def getLR: Double = this.lr

  def getRegL1Param: Double = this.regL1Param

  def getRegL2Param: Double = this.regL2Param

  def toJson: JObject
}


object Optimizer {

  abstract class Json2OptimizerProvider {
    def optFromJson(json: JValue): Optimizer

    def defaultOptJson(): JObject
  }

  def getJson2OptimizerProvider(className: String): Json2OptimizerProvider = {
    val cls = Class.forName(className)
    cls.newInstance().asInstanceOf[Json2OptimizerProvider]
  }

  def getJson2OptimizerProvider(cls: Class[_<: Json2OptimizerProvider]): Json2OptimizerProvider = {
    cls.newInstance()
  }
}


abstract class GradientDescent(val lr: Double = 0.1) extends Optimizer {
  setLR(lr)
}
