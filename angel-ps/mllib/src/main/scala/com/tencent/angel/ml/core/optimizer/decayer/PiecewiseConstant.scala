package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

class PiecewiseConstant(eta:Double, boundaries:Array[Int], values: Array[Double]) extends StepSizeScheduler {
  assert(boundaries.length == values.length)

  private var current: Int = 0
  private var currIdx: Int = 0
  private val interval: Int = SharedConf.get().getInt(MLConf.ML_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1

    if (current >= boundaries(currIdx)) {
      currIdx += 1
    }

    eta * values(currIdx)
  }

  override def isIntervalBoundary: Boolean = {
    current % interval == 0
  }
}
