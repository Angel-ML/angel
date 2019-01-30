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

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.utils.OptimizerKeys
import com.tencent.angel.ml.core.variable.Updater
import org.json4s.JsonAST.{JDouble, JObject, JValue}


trait Optimizer extends Updater with Serializable {
  var lr: Double
  val epsilon: Double = 1e-10
  val conf: SharedConf = SharedConf.get()

  protected var regL1Param: Double = conf.getDouble(MLCoreConf.ML_REG_L1, MLCoreConf.DEFAULT_ML_REG_L1)
  protected var regL2Param: Double = conf.getDouble(MLCoreConf.ML_REG_L2, MLCoreConf.DEFAULT_ML_REG_L2)

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
  def getOptimizerProvider(className: String): OptimizerProvider = {
    val cls = Class.forName(className)
    cls.newInstance().asInstanceOf[OptimizerProvider]
  }

  def getOptimizerProvider(cls: Class[_<: OptimizerProvider]): OptimizerProvider = {
    cls.newInstance()
  }
}

trait OptimizerHelper {
  val conf: SharedConf = SharedConf.get()

  def setRegParams[T <: Optimizer](opt: T, jast: JValue): T = {
    jast \ OptimizerKeys.reg1Key match {
      case JDouble(num) => opt.setRegL1Param(num)
      case _ => opt.setRegL1Param(conf.getDouble(MLCoreConf.ML_REG_L1, MLCoreConf.DEFAULT_ML_REG_L1))
    }

    jast \ OptimizerKeys.reg2Key match {
      case JDouble(num) => opt.setRegL2Param(num)
      case _ => opt.setRegL1Param(conf.getDouble(MLCoreConf.ML_REG_L2, MLCoreConf.DEFAULT_ML_REG_L2))
    }

    opt
  }
}

trait OptimizerProvider {
  def optFromJson(json: JValue): Optimizer

  def defaultOptJson(): JObject
}