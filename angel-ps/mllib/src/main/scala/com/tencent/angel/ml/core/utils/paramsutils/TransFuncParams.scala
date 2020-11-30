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
import com.tencent.angel.ml.core.network.transfunc._
import org.json4s.{DefaultFormats, JNothing, JString, JValue}

class TransFuncParams(val name: String) {
  def build(): TransFunc = {
    name.trim.toLowerCase match {
      case "identity" => new Identity()
      case "sigmoid" => new Sigmoid()
      case "relu" => new Relu()
      case "tanh" => new Tanh()
      case "softmax" => new Softmax()
      case tfNmae => throw new AngelException(s"Unknow TransFunc $tfNmae")
    }
  }
}

object TransFuncParams {
  implicit val formats = DefaultFormats

  val sigmoidWithDropout: String = classOf[SigmoidWithDropout].getSimpleName.toLowerCase
  val tanhWithDropout: String = classOf[TanhWithDropout].getSimpleName.toLowerCase
  val dropout: String = classOf[Dropout].getSimpleName.toLowerCase

  def apply(json: JValue, default: Option[TransFuncParams] = None): TransFuncParams = {
    json match {
      case JNothing if default.isDefined => default.getOrElse(new TransFuncParams("sigmoid"))
      case trans: JString if trans.extract[String].trim.toLowerCase.contains("dropout") =>
        DropoutTransFuncParams(trans.extract[String].trim, Some(0.5), Some("train"))
      case trans: JString =>
        new TransFuncParams(trans.extract[String].trim)
      case _: JValue =>
        val name = (json \ ParamKeys.typeName).extract[String].trim

        if (name.toLowerCase.contains("dropout")) {
          val proba: Option[Double] = json \ ParamKeys.proba match {
            case JNothing => Some(0.5)
            case v: JValue => Some(v.extract[Double])
          }

          val actionType: Option[String] = json \ ParamKeys.actionType match {
            case JNothing => Some("train")
            case v: JValue => Some(v.extract[String])
          }

          DropoutTransFuncParams(name, proba, actionType)
        } else {
          new TransFuncParams(name)
        }
    }
  }
}

case class DropoutTransFuncParams(override val name: String, proba: Option[Double], actionType: Option[String])
  extends TransFuncParams(name) {
  override def build(): TransFunc = {
    import TransFuncParams._
    name.trim.toLowerCase match {
      case `sigmoidWithDropout` => new SigmoidWithDropout(proba.getOrElse(0.5), actionType.getOrElse("train"))
      case `tanhWithDropout` => new TanhWithDropout(proba.getOrElse(0.5), actionType.getOrElse("train"))
      case `dropout` => new Dropout(proba.getOrElse(0.5), actionType.getOrElse("train"))
      case tfNmae => throw new AngelException(s"Unknow TransFunc $tfNmae")
    }
  }
}
