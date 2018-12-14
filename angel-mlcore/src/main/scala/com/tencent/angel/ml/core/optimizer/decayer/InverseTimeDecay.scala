package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class InverseTimeDecay(eta: Double, staircase:Boolean = false) extends StepSizeScheduler {
  private var current: Int = 0
  private val decay: Double = SharedConf.decay
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1

    val ratio: Double = if (staircase) {
      current / interval
    } else {
      current.toDouble / interval
    }

    eta / (1.0 + decay * ratio)
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
