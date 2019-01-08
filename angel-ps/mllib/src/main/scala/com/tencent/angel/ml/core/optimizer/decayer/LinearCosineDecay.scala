package com.tencent.angel.ml.core.optimizer.decayer

import java.util.Random

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class LinearCosineDecay(eta: Double, alpha: Double = 0.0, beta: Double = 0.001, useNoisy: Boolean = false) extends StepSizeScheduler {
  private var current: Int = 0
  private val numPeriods: Double = 0.5
  private val initialVariance: Double = 1.0
  private val varianceDecay: Double = 0.55
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_OPT_DECAY_INTERVALS, 100)
  private val rand = new Random()


  override def next(): Double = {
    current += 1
    val ratio: Double = Math.min(current, interval).toDouble / interval
    val linearDecay: Double = 1 - ratio
    val cosineDecay: Double = (1 + Math.cos(Math.PI * 2 * numPeriods * ratio)) / 2

    val esp: Double = if (useNoisy) {
      val std: Double = Math.sqrt(initialVariance / Math.pow(1 + current, varianceDecay))
      rand.nextGaussian() * std
    } else {
      0.0
    }

    var decayed: Double = (alpha + linearDecay + esp) * cosineDecay + beta
    if (decayed < 0) {
      decayed = beta
    }

    eta * decayed
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
