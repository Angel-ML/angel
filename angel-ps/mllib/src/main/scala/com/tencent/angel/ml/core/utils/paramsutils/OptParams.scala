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
import com.tencent.angel.ml.core.optimizer._
import org.json4s.{DefaultFormats, JNothing, JString, JValue}

class OptParams(val name: String, val lr: Option[Double], val reg1: Option[Double], val reg2: Option[Double]) {
  def build(): Optimizer = {
    name.trim.toLowerCase match {
      case "sgd" =>
        val opt = new SGD(lr.getOrElse(1.0))
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

  private def nameMatch(json: JValue, name: String): Boolean = {
    json.extract[String].trim.equalsIgnoreCase(name)
  }

  def apply(json: JValue, default: Option[OptParams] = None): OptParams = {
    json match {
      case JNothing if default.isDefined => default.get
      case JNothing if default.isEmpty => MomentumParams(momentum, Some(1.0), Some(0.9), None, None)
      case jast: JString if nameMatch(jast, adam) =>
        AdamParams(jast.extract[String].trim, Some(1.0), Some(0.9), Some(0.99), None, None)
      case jast: JString if nameMatch(jast, ftrl) =>
        FTRLParams(jast.extract[String].trim, Some(1.0), Some(0.1), Some(0.02), None, None)
      case jast: JString if nameMatch(jast, momentum) =>
        MomentumParams(jast.extract[String].trim, Some(1.0), Some(0.9), None, None)
      case jast: JString if nameMatch(jast, sgd) =>
        new OptParams(jast.extract[String].trim, Some(1.0), None, None)
      case jast: JString => throw new AngelException(s"No such a optimizer: ${jast.extract[String]}!")
      case _: JValue => // for dictionary fashion
        val optType = (json \ ParamKeys.typeName).extract[String].trim

        val learningRate: Option[Double] = json \ ParamKeys.lr match {
          case JNothing => Some(1.0)
          case lr: JValue => Some(lr.extract[Double])
        }

        val reg1: Option[Double] = json \ ParamKeys.reg1 match {
          case JNothing => None
          case r1: JValue => Some(r1.extract[Double])
        }

        val reg2: Option[Double] = json \ ParamKeys.reg2 match {
          case JNothing => None
          case r2: JValue => Some(r2.extract[Double])
        }

        optType.toLowerCase match {
          case `adam` =>
            val gamma: Option[Double] = json \ ParamKeys.gamma match {
              case JNothing => Some(0.99)
              case v: JValue => Some(v.extract[Double])
            }

            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(0.9)
              case v: JValue => Some(v.extract[Double])
            }

            AdamParams(optType, learningRate, gamma, beta, reg1, reg2)
          case `ftrl` =>
            val alpha: Option[Double] = json \ ParamKeys.alpha match {
              case JNothing => Some(0.1)
              case v: JValue => Some(v.extract[Double])
            }

            val beta: Option[Double] = json \ ParamKeys.beta match {
              case JNothing => Some(0.2)
              case v: JValue => Some(v.extract[Double])
            }

            FTRLParams(optType, learningRate, alpha, beta, reg1, reg2)
          case `momentum` =>
            val momentum: Option[Double] = json \ ParamKeys.momentum match {
              case JNothing => Some(0.9)
              case v: JValue => Some(v.extract[Double])
            }

            MomentumParams(optType, learningRate, momentum, reg1, reg2)
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
    val opt = new Adam(lr.getOrElse(1.0), gamma.getOrElse(0.99), beta.getOrElse(0.9))
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL1Param(r2))
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
    val opt = new FTRL(lr.getOrElse(1.0), alpha.getOrElse(0.1), beta.getOrElse(0.02))
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL1Param(r2))
    opt
  }
}

case class MomentumParams(override val name: String,
                          override val lr: Option[Double],
                          momentum: Option[Double],
                          override val reg1: Option[Double],
                          override val reg2: Option[Double]) extends OptParams(name, lr, reg1, reg2) {
  override def build(): Optimizer = {
    val opt = new Momentum(lr.getOrElse(1.0), momentum.getOrElse(0.9))
    reg1.foreach(r1 => opt.setRegL1Param(r1))
    reg2.foreach(r2 => opt.setRegL1Param(r2))
    opt
  }
}
