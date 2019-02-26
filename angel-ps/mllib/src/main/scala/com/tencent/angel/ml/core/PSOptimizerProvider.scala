package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.optimizer._
import com.tencent.angel.ml.core.utils.JsonUtils.{fieldEqualClassName, matchClassName}
import com.tencent.angel.ml.core.utils.OptimizerKeys
import org.json4s.JsonAST.{JDouble, JObject, JString, JValue}
import org.json4s.JsonDSL._

class PSOptimizerProvider extends OptimizerProvider with OptimizerHelper {
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

  override def optFromJson(json: JValue): Optimizer = {
    val conf: SharedConf = SharedConf.get()
    val lr0: Double = conf.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)

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
        SGD.fromJson(obj)
      case obj: JObject if fieldEqualClassName[Adam](obj) =>
        Adam.fromJson(obj)
      case obj: JObject if fieldEqualClassName[Momentum](obj) =>
        Momentum.fromJson(obj)
      case obj: JObject if fieldEqualClassName[FTRL](obj) =>
        FTRL.fromJson(obj)
      case obj: JObject if fieldEqualClassName[AdaGrad](obj) =>
        AdaGrad.fromJson(obj)
      case obj: JObject if fieldEqualClassName[AdaDelta](obj) =>
        AdaDelta.fromJson(obj)
      case _ =>
        val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
          MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)
        new Momentum(lr0, momentum)
    }

    setRegParams(opt, json)
  }

  override def defaultOptJson(): JObject = {
    val conf: SharedConf = SharedConf.get()
    val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
      MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)

    (OptimizerKeys.typeKey -> s"${classOf[Momentum].getSimpleName}") ~
      (OptimizerKeys.momentumKey -> JDouble(momentum))
  }

}
