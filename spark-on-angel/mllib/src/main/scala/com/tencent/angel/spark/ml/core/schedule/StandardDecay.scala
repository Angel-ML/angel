package com.tencent.angel.spark.ml.core.schedule

class StandardDecay(eta: Double, decay: Double) extends StepSizeScheduler {

  var current: Int = 0

  override def next(): Double = {
    current += 1
    return eta / math.sqrt(1.0 + decay * current)
  }

}
