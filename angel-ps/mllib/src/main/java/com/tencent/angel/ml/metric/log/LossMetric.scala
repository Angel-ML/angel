package com.tencent.angel.ml.metric.log

import com.tencent.angel.ml.metrics.Metric


class LossMetric(var sampleNum:Int) extends Metric {
  var globalLoss: Double = 0.0

  override def merge(other: Metric): Metric = {
    this.sampleNum += other.asInstanceOf[LossMetric].sampleNum
    this.globalLoss += other.asInstanceOf[LossMetric].globalLoss
    this
  }

  override def calculate: Double = {
    this.globalLoss / this.sampleNum
  }

  override def setValues(values: Double*): Unit = {
    this.globalLoss = values(0)
  }

  override def toString: String = {
    DEFAULT_METRIC_FORMAT.format(this.calculate)
  }
}

object LossMetric{
  def apply(sampleNum:Int) = {
    new LossMetric(sampleNum)
  }
}
