package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class CorrectionDecay(eta: Double, alpha: Double, beta: Double) extends StepSizeScheduler {

  var current: Int = 0
  val interval: Int = SharedConf.get().getInt(MLConf.ML_OPT_DECAY_INTERVALS, 100)

  override def next(): Double = {
    val eta_time = eta / math.sqrt(1 + alpha * current) * (1 - beta) / (1 - math.pow(beta, current))
    current += 1

    eta_time
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }

}
