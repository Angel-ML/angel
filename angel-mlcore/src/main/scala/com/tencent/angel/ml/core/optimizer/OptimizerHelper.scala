package com.tencent.angel.ml.core.optimizer

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.utils.OptimizerKeys
import org.json4s.JsonAST.{JDouble, JValue}

trait OptimizerHelper {
  val conf: SharedConf = SharedConf.get()

  def setRegParams[T <: Optimizer](opt: T, jast: JValue): T = {
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