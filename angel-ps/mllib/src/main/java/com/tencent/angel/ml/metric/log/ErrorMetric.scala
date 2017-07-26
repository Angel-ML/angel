package com.tencent.angel.ml.metric.log

import com.tencent.angel.ml.metrics.Metric

class ErrorMetric(var sampleNum: Int) extends Metric{

  var globalErr: Double = 0.0

  override def merge(other: Metric): Metric = {
    this.sampleNum += other.asInstanceOf[ErrorMetric].sampleNum
    this.globalErr += (other.asInstanceOf[ErrorMetric].globalErr - this.globalErr) *
      other.asInstanceOf[ErrorMetric].sampleNum / (other.asInstanceOf[ErrorMetric].sampleNum + this.sampleNum)
    this
  }

  override def calculate: Double = {
    this.globalErr
  }

  override def setValues(values: Double*): Unit = {
    this.globalErr = values(0)
  }

  override def toString: String = {
    DEFAULT_METRIC_FORMAT.format(this.calculate)
  }
}

object ErrorMetric{
  def apply(sampleNum:Int) = {
    new ErrorMetric(sampleNum)
  }

}
