package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class CosineDecayRestarts(eta: Double, alpha: Double = 0.001, tMul:Double=2.0, mMul:Double=1.0) extends StepSizeScheduler {
  assert(tMul >= 1.0 && mMul <= 1.0)
  private var current: Int = 0
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1
    val ratio: Double = current.toDouble / interval

    val temp = if (tMul == 1.0) {
      computeStep(ratio)
    } else {
      computeStep(ratio, geometric = true)
    }

    val mFac = Math.pow(mMul, temp._1)

    val cosineDecay: Double = mFac * (1.0 + Math.cos(Math.PI * ratio)) / 2.0
    val decayed = (1-alpha) * cosineDecay + alpha
    eta * decayed
  }

  private def computeStep(completedFraction: Double, geometric:Boolean=false):(Double, Double) = {
    if (geometric) {
      val iRestart = Math.floor(Math.log(1.0 - completedFraction * (1.0 - tMul)) / Math.log(tMul))
      val sumR = (1.0 - Math.pow(tMul, iRestart)) / (1.0 - tMul)
      (iRestart, (completedFraction - sumR) / Math.pow(tMul, iRestart))
    } else {
      val iRestart = Math.floor(completedFraction)
      (iRestart, completedFraction-iRestart)
    }
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
