package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.local.optimizer.{Adam, Momentum, SGD}
import com.tencent.angel.ml.core.optimizer.{Optimizer, OptimizerProvider}
import com.tencent.angel.ml.core.utils.{LayerKeys, OptimizerKeys}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JDouble, JNothing, JObject, JString, JValue}
import org.json4s.ParserUtil.ParseException
import org.json4s.native.JsonMethods._

import scala.reflect.ClassTag

class LocalOptimizerProvider extends OptimizerProvider {
  private implicit val formats: DefaultFormats.type = DefaultFormats

  def json2String(obj: JValue): String = {
    obj \ LayerKeys.optimizerKey match {
      case JString(opt) => opt
      case opt: JObject => pretty(render(opt))
      case _ => "Momentum"
    }
  }

  def string2Json(jsonstr: String): JValue = {
    try {
      parse(jsonstr)
    } catch {
      case _: ParseException =>
        if (jsonstr.startsWith("\"")) {
          JString(jsonstr.substring(1, jsonstr.length - 1))
        } else {
          JString(jsonstr)
        }
      case e: Exception => JNothing
    }
  }

  override def optFromJson(jsonstr: String): Optimizer = {
    val json = string2Json(jsonstr)

    val opt = json match {
      case JString(s) if matchClassName[SGD](s) =>
        new SGD(lr = 0.0001)
      case JString(s) if matchClassName[Adam](s) =>
        new Adam(lr = 0.0001, beta = 0.9, gamma = 0.99)
      case JString(s) if matchClassName[Momentum](s) =>
        new Momentum(lr = 0.0001, momentum = 0.9)
      case obj: JObject if fieldEqualClassName[SGD](obj) =>
        SGD.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[Adam](obj) =>
        Adam.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[Momentum](obj) =>
        Momentum.fromJson(obj, this)
      case _ => new Momentum(lr = 0.0001, momentum = 0.9)
    }

    setRegParams(opt, pretty(render(json)))
  }

  override def setRegParams[T <: Optimizer](opt: T, jastStr: String): T = {
    val jast = string2Json(jastStr)
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

  def extract[T: Manifest](jast: JValue, key: String, default: Option[T] = None): Option[T] = {
    jast \ key match {
      case JNothing => default
      case value => Some(value.extract[T](formats, implicitly[Manifest[T]]))
    }
  }

  def matchClassName[T: ClassTag](name: String): Boolean = {
    val runtimeClassName = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    runtimeClassName.equalsIgnoreCase(name)
  }

  def fieldEqualClassName[T: ClassTag](obj: JObject, fieldName: String = "type"): Boolean = {
    val runtimeClassName = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    val name = extract[String](obj, fieldName)
    if (name.isEmpty) {
      false
    } else {
      runtimeClassName.equalsIgnoreCase(name.get)
    }
  }
}
