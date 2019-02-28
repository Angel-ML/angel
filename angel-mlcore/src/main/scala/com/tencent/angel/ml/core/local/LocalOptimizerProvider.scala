package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.optimizer.{Adam, Momentum, SGD}
import com.tencent.angel.ml.core.optimizer.{Optimizer, OptimizerHelper, OptimizerProvider}
import com.tencent.angel.ml.core.utils.JsonUtils.{fieldEqualClassName, matchClassName}
import org.json4s.JsonAST.{JDouble, JObject, JString, JValue}
import org.json4s.JsonDSL._


class LocalOptimizerProvider extends OptimizerProvider with OptimizerHelper {
  def optFromJson(json: JValue): Optimizer = {
    val opt = json match {
      case JString(s) if matchClassName[SGD](s) =>
        new SGD(lr = 0.0001)
      case JString(s) if matchClassName[Adam](s) =>
        new Adam(lr = 0.0001, beta = 0.9, gamma = 0.99)
      case JString(s) if matchClassName[Momentum](s) =>
        new Momentum(lr = 0.0001, momentum = 0.9)
      case obj: JObject if fieldEqualClassName[SGD](obj) =>
        SGD.fromJson(obj)
      case obj: JObject if fieldEqualClassName[Adam](obj) =>
        Adam.fromJson(obj)
      case obj: JObject if fieldEqualClassName[Momentum](obj) =>
        Momentum.fromJson(obj)
      case _ => new Momentum(lr = 0.0001, momentum = 0.9)
    }

    setRegParams(opt, json)
  }

  def defaultOptJson(): JObject = {
    ("type" -> s"${classOf[Momentum].getSimpleName}") ~
      ("momentum" -> JDouble(0.9))
  }
}
