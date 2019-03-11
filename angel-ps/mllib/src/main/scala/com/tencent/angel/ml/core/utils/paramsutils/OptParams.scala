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


package com.tencent.angel.ml.core.utils.paramsutils

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.optimizer._
import org.json4s.{DefaultFormats, JNothing, JString, JValue}

class OptParams(val name: String, val lr: Option[Double], val reg1: Option[Double], val reg2: Option[Double]) {
  def build(): Optimizer = {
    name.trim.toLowerCase match {
      case "sgd" =>
        val opt = new SGD(lr.get)
        reg1.foreach(r1 => opt.setRegL1Param(r1))
        reg2.foreach(r2 => opt.setRegL2Param(r2))
        opt
      case _ => throw new AngelException(s"Unknown Optimizer $name !")
    }
  }
}

object OptParams {
  implicit val formats = DefaultFormats
  val momentum: String = "momentum"
  val adam: String = "adam"
  val ftrl: String = "ftrl"
  val sgd: String = "sgd"
  val adagrad: String = "adagrad"
  val adadelta: String = "adadelta"

  private def nameMatch(json: JValue, name: String): Boolean = {
    json.extract[String].trim.equalsIgnoreCase(name)
  }

  def apply(json: JValue, default: Option[OptParams] = None): OptParams = {
    val conf = SharedConf.get()
    val lr = Some(SharedConf.learningRate)
      json match {
      case JNothing if default.isDefined => default.get
      case JNothing if default.isEmpty =>
        val momentumValue = Some(conf.getDouble(MLConf.ML_OPT_MOMENTUM_MOMENTUM))
        MomentumParams(momentum, lr, momentumValue, None, None)
      case jast: JString if nameMatch(jast, adam) =>
        val gamma = Some(conf.getDouble(MLConf.ML_OPT_ADAM_GAMMA))
        val beta = Some(conf.getDouble(MLConf.ML_OPT_ADAM_BETA))
        AdamParams(jast.extract[String].trim, lr, gamma, beta, None, None)
      case jast: JString if nameMatch(jast, ftrl) =>
        val alpha = Some(conf.getDouble(MLConf.ML_OPT_FTRL_ALPHA))
        val beta = Some(conf.getDouble(MLConf.ML_OPT_FTRL_BETA))
        FTRLParams(jast.extract[String].trim, lr, alpha, beta, None, None)
      case jast: JString if nameMatch(jast, momentum) =>
        val momentumValue = Some(conf.getDouble(MLConf.ML_OPT_MOMENTUM_MOMENTUM))
        MomentumParams(jast.extract[String].trim, lr, momentumValue, None, None)
      case jast: JString if nameMatch(jast, adagrad) =>
        val beta = Some(conf.getDouble(MLConf.ML_OPT_ADAGRAD_BETA))
        AdaGradParams(jast.extract[String].trim, lr, beta, None, None)
      case jast: JString if nameMatch(jast, adadelta) =>
        val alpha = Some(conf.getDouble(MLConf.ML_OPT_ADADELTA_ALPHA))
        val beta = Some(conf.getDouble(MLConf.ML_OPT_ADADELTA_BETA))
        AdaDeltaParams(jast.extract[String].trim, lr, alpha, beta, None, None)
      case jast: JString if nameMatch(jast, sgd) =>
        new OptParams(jast.extract[String].trim, lr, None, None)
      case jast: JString => throw new AngelException(s"No such a optimizer: ${jast.extract[String]}!")
      case _: JValue => // for dictionary fashion
        val optType = (json \ ParamKeys.typeName).extract[String].trim

        val learningRate: Option[Double] = json \ ParamKeys.lr match {
          case JNothing => lr
          case lr: JValue => Some(lr.extract[Double])
        }

        val reg1: Option[Double] = json \ ParamKeys.reg1 match {
          case JNothing => Some(conf.getDouble(MLConf.ML_REG_L1))
          case r1: JValue => Some(r1.extract[Double])
        }

        val reg2: Option[Double] = json \ ParamKeys.reg2 match {
          case JNothing => Some(conf.getDouble(MLConf.ML_REG_L2))
          case r2: JValue => Some(r2.extract[Double])
        }

        optType.toLowerCase match {
          case `adam` =>
            val gamma: Option[Double] = json \ ParamKeys.gamma match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_ADAM_GAMMA))
              case v: JValue => Some(v.extract[Double])
            }

            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_ADAM_BETA))
              case v: JValue => Some(v.extract[Double])
            }

            AdamParams(optType, learningRate, gamma, beta, reg1, reg2)
          case `ftrl` =>
            val alpha: Option[Double] = json \ ParamKeys.alpha match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_FTRL_ALPHA))
              case v: JValue => Some(v.extract[Double])
            }

            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_FTRL_BETA))
              case v: JValue => Some(v.extract[Double])
            }

            FTRLParams(optType, learningRate, alpha, beta, reg1, reg2)
          case `momentum` =>
            val momentum: Option[Double] = json \ ParamKeys.momentum match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_MOMENTUM_MOMENTUM))
              case v: JValue => Some(v.extract[Double])
            }

            MomentumParams(optType, learningRate, momentum, reg1, reg2)
          case `adagrad` =>
            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_ADAGRAD_BETA))
              case v: JValue => Some(v.extract[Double])
            }

            AdaGradParams(optType, learningRate, beta, reg1, reg2)
          case `adadelta` =>
            val alpha: Option[Double] = json \ ParamKeys.alpha match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_ADADELTA_ALPHA))
              case v: JValue => Some(v.extract[Double])
            }

            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(conf.getDouble(MLConf.ML_OPT_ADADELTA_BETA))
              case v: JValue => Some(v.extract[Double])
            }

            AdaDeltaParams(optType, learningRate, alpha, beta, reg1, reg2)
          case `sgd` =>
            new OptParams(optType, learningRate, reg1, reg2)
          case _ => throw new AngelException(s"No such a optimizer: $optType!")
        }
    }
  }
}

case class AdamParams(override val name: String,
                      override val lr: Option[Double],
                      gamma: Option[Double],
                      beta: Option[Double],
                      override val reg1: Option[Double],
                      override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new Adam(lr.get, gamma.get, beta.get)
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL2Param(r2))
    opt
  }
}

case class FTRLParams(override val name: String,
                      override val lr: Option[Double],
                      alpha: Option[Double],
                      beta: Option[Double],
                      override val reg1: Option[Double],
                      override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new FTRL(lr.get, alpha.get, beta.get)
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL2Param(r2))
    opt
  }
}

case class MomentumParams(override val name: String,
                          override val lr: Option[Double],
                          momentum: Option[Double],
                          override val reg1: Option[Double],
                          override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new Momentum(lr.get, momentum.get)
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL2Param(r2))
    opt
  }
}

case class AdaGradParams(override val name: String,
                         override val lr: Option[Double],
                         beta: Option[Double],
                         override val reg1: Option[Double],
                         override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new AdaGrad(lr.get, beta.get)
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL2Param(r2))
    opt
  }
}

case class AdaDeltaParams(override val name: String,
                          override val lr: Option[Double],
                          alpha: Option[Double],
                          beta: Option[Double],
                          override val reg1: Option[Double],
                          override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new AdaDelta(lr.get, alpha.get, beta.get)
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL2Param(r2))
    opt
  }
}
