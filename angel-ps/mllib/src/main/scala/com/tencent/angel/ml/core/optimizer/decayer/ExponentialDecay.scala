package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class ExponentialDecay(eta: Double, alpha: Double, staircase: Boolean = false, useNatural: Boolean = false) extends StepSizeScheduler {
  private var current: Int = 0
  private val decay: Double = if (useNatural) {
    Math.exp(-alpha)
  } else {
    alpha
  }

  private val interval: Int = SharedConf.get().getInt(MLConf.ML_OPT_DECAY_INTERVALS, 100)

  assert(interval > 0)

  override def next(): Double = {
    current += 1

    val p: Double = if (staircase) {
      current / interval
    } else {
      current.toDouble / interval
    }

    eta * Math.pow(decay, p)
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
