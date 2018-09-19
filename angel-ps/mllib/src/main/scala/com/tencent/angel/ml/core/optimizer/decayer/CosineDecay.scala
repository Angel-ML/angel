package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class CosineDecay(eta: Double, alpha: Double = 0.001) extends StepSizeScheduler {
  private var current: Int = 0
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1
    val ratio: Double = Math.min(current, interval).toDouble / interval
    val cosineDecay: Double = (1 + Math.cos(Math.PI * ratio)) / 2
    val decayed = (1-alpha) * cosineDecay + alpha

    eta * decayed
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
