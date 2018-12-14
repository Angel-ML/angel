package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.optimizer.{Adam, Momentum, SGD}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.JsonUtils.{fieldEqualClassName, matchClassName}
import org.json4s.JsonAST.{JDouble, JObject, JString, JValue}
import org.json4s.JsonDSL._


object OptimizerKeys {
  val typeKey: String = "type"
  val betaKey: String = "beta"
  val gammaKey: String = " gamma"
}


class OptJsonProvider extends Optimizer.Json2OptimizerProvider {
  def optFromJson(json: JValue): Optimizer = {
    json match {
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
  }

  def defaultOptJson(): JObject = {
    ("type" -> s"${classOf[Momentum].getSimpleName}") ~
      ("momentum" -> JDouble(0.9))
  }
}
