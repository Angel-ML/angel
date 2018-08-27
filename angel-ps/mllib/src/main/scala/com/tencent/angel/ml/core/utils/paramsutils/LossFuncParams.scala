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
import com.tencent.angel.ml.core.optimizer.loss._
import org.json4s.{DefaultFormats, JDouble, JNothing, JString, JValue}

class LossFuncParams(val name: String) {
  def build(): LossFunc = {
    name.trim.toLowerCase match {
      case "l2loss" => new L2Loss()
      case "logloss" => new LogLoss()
      case "hingeloss" => new HingeLoss()
      case "crossentropyloss" => new CrossEntropyLoss()
      case "softmaxloss" => new SoftmaxLoss()
      case lfName => throw new AngelException(s"Uknown LossFunc $lfName")
    }
  }
}

object LossFuncParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue, default: Option[LossFuncParams] = None): LossFuncParams = {
    json match {
      case JNothing if default.isDefined => default.get
      case JNothing if default.isEmpty => new LossFuncParams("LogLoss")
      case loss: JString if loss.extract[String].trim.equalsIgnoreCase("huberloss") =>
        HuberLossParams(loss.extract[String].trim, Some(1.0))
      case loss: JString =>
        new LossFuncParams(loss.extract[String].trim)
      case _: JValue =>
        val name = (json \ ParamKeys.typeName).extract[String].trim

        if (name.equalsIgnoreCase("huberloss")) {
          val delta: Option[Double] = json \ ParamKeys.delta match {
            case JNothing => Some(1.0)
            case loss: JDouble => Some(loss.extract[Double])
          }
          HuberLossParams(name, delta)
        } else {
          new LossFuncParams(name)
        }
    }
  }
}

case class HuberLossParams(override val name: String, delta: Option[Double] = None) extends LossFuncParams(name) {
  override def build(): LossFunc = {
    new HuberLoss(delta.get)
  }
}
