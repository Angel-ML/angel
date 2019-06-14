package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.optimizer._
import com.tencent.angel.ml.core.utils.{LayerKeys, OptimizerKeys}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JDouble, JNothing, JObject, JString, JValue}
import org.json4s.ParserUtil.ParseException
import org.json4s.native.JsonMethods._

import scala.reflect.ClassTag

class PSOptimizerProvider extends OptimizerProvider {
  private implicit val formats: DefaultFormats.type = DefaultFormats

  def extract[T: Manifest](jast: JValue, key: String, default: Option[T] = None): Option[T] = {
    jast \ key match {
      case JNothing => default
      case value => Some(value.extract[T](formats, implicitly[Manifest[T]]))
    }
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

  def matchClassName[T: ClassTag](name: String): Boolean = {
    val runtimeClassName = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    runtimeClassName.equalsIgnoreCase(name)
  }

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

  def getOptimizer(name: String): Optimizer = {
    val conf: SharedConf = SharedConf.get()
    val lr0: Double = conf.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)

    name match {
      case s: String if matchClassName[SGD](s) =>
        new SGD(lr = lr0)
      case s: String if matchClassName[Adam](s) =>
        val gamma: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAM_GAMMA,
          MLCoreConf.DEFAULT_ML_OPT_ADAM_GAMMA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAM_BETA, MLCoreConf.DEFAULT_ML_OPT_ADAM_BETA)
        new Adam(lr0, gamma, beta)
      case s: String if matchClassName[Momentum](s) =>
        val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
          MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)
        new Momentum(lr0, momentum)
      case s: String if matchClassName[FTRL](s) =>
        val alpha: Double = conf.getDouble(MLCoreConf.ML_OPT_FTRL_ALPHA,
          MLCoreConf.DEFAULT_ML_OPT_FTRL_ALPHA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_FTRL_BETA,
          MLCoreConf.DEFAULT_ML_OPT_FTRL_BETA)
        new FTRL(lr0, alpha, beta)
      case s: String if matchClassName[AdaGrad](s) =>
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAGRAD_BETA,
          MLCoreConf.DEFAULT_ML_OPT_ADAGRAD_BETA)
        new AdaGrad(lr0, beta)
      case s: String if matchClassName[AdaDelta](s) =>
        val alpha: Double = conf.getDouble(MLCoreConf.ML_OPT_ADADELTA_ALPHA,
          MLCoreConf.DEFAULT_ML_OPT_ADADELTA_ALPHA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADADELTA_BETA,
          MLCoreConf.DEFAULT_ML_OPT_ADADELTA_BETA)
        new AdaDelta(lr0, alpha, beta)
    }
  }

  def getDefaultOptimizer(): Optimizer = {
    val conf: SharedConf = SharedConf.get()
    val lr0: Double = conf.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)
    val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
      MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)
    new Momentum(lr0, momentum)
  }

  override def optFromJson(jsonstr: String): Optimizer = {
    val conf: SharedConf = SharedConf.get()
    val lr0: Double = conf.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)

    val json = string2Json(jsonstr)
    val opt = json match {
      case JString(s) if matchClassName[SGD](s) =>
        new SGD(lr = lr0)
      case JString(s) if matchClassName[Adam](s) =>
        val gamma: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAM_GAMMA,
          MLCoreConf.DEFAULT_ML_OPT_ADAM_GAMMA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAM_BETA, MLCoreConf.DEFAULT_ML_OPT_ADAM_BETA)
        new Adam(lr0, gamma, beta)
      case JString(s) if matchClassName[Momentum](s) =>
        val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
          MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)
        new Momentum(lr0, momentum)
      case JString(s) if matchClassName[FTRL](s) =>
        val alpha: Double = conf.getDouble(MLCoreConf.ML_OPT_FTRL_ALPHA,
          MLCoreConf.DEFAULT_ML_OPT_FTRL_ALPHA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_FTRL_BETA,
          MLCoreConf.DEFAULT_ML_OPT_FTRL_BETA)
        new FTRL(lr0, alpha, beta)
      case JString(s) if matchClassName[AdaGrad](s) =>
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADAGRAD_BETA,
          MLCoreConf.DEFAULT_ML_OPT_ADAGRAD_BETA)
        new AdaGrad(lr0, beta)
      case JString(s) if matchClassName[AdaDelta](s) =>
        val alpha: Double = conf.getDouble(MLCoreConf.ML_OPT_ADADELTA_ALPHA,
          MLCoreConf.DEFAULT_ML_OPT_ADADELTA_ALPHA)
        val beta: Double = conf.getDouble(MLCoreConf.ML_OPT_ADADELTA_BETA,
          MLCoreConf.DEFAULT_ML_OPT_ADADELTA_BETA)
        new AdaDelta(lr0, alpha, beta)
      case obj: JObject if fieldEqualClassName[SGD](obj) =>
        SGD.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[Adam](obj) =>
        Adam.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[Momentum](obj) =>
        Momentum.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[FTRL](obj) =>
        FTRL.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[AdaGrad](obj) =>
        AdaGrad.fromJson(obj, this)
      case obj: JObject if fieldEqualClassName[AdaDelta](obj) =>
        AdaDelta.fromJson(obj, this)
      case _ =>
        val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
          MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)
        new Momentum(lr0, momentum)
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
}
