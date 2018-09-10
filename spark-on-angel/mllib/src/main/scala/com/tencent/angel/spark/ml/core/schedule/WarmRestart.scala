package com.tencent.angel.spark.ml.core.schedule

class WarmRestart(etaMax: Double, etaMin: Double, var interval: Int) extends StepSizeScheduler {

  var current: Int = 0
  override def next(): Double = {
    current += 1
    val value = etaMin + 0.5 * (etaMax - etaMin) * (1 + math.cos(((current * 1.0) / interval * math.Pi)))
    if (current == interval) {
      current = 0
      interval *= 2
    }
    return value
  }

}
