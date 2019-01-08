package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class PolynomialDecay(eta:Double, endingEta: Double, power: Double, cycle:Boolean=false) extends StepSizeScheduler{
  private var current: Int = 0
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_OPT_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1
    val ratio: Double = if (cycle) {
      val temp = (current + interval -1) / interval * interval
      current.toDouble / temp
    } else {
      Math.min(current, interval).toDouble / interval
    }

    (eta - endingEta) * Math.pow(1.0 - ratio, power) + endingEta
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
