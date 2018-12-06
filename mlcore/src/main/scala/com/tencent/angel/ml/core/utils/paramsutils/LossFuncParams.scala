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

import com.tencent.angel.ml.core.optimizer.loss._
import com.tencent.angel.ml.core.utils.MLException
import org.json4s.{DefaultFormats, JNothing, JString, JValue}
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils.extract

class LossFuncParams(val name: String, delta: Option[Double] = Some(1.0)) {
  def build(): LossFunc = {
    name.trim.toLowerCase match {
      case "l2loss" => new L2Loss()
      case "logloss" => new LogLoss()
      case "hingeloss" => new HingeLoss()
      case "crossentropyloss" => new CrossEntropyLoss()
      case "softmaxloss" => new SoftmaxLoss()
      case "huberloss" => new HuberLoss(delta.get)
      case lfName => throw MLException(s"Uknown LossFunc $lfName")
    }
  }
}

object LossFuncParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue, default: Option[LossFuncParams] = None): LossFuncParams = {
    json match {
      case JNothing => if (default.isDefined) default.get else new LossFuncParams("LogLoss")
      case loss: JString =>
        new LossFuncParams(loss.extract[String])
      case _: JValue =>
        val name = extract[String](json, ParamKeys.typeName).get
        if (name.equalsIgnoreCase("huberloss")) {
          new LossFuncParams(name, extract[Double](json, ParamKeys.delta))
        } else {
          new LossFuncParams(name)
        }
    }
  }
}
