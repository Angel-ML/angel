package com.tencent.angel.ml.metric.log

import com.tencent.angel.ml.metrics.Metric

class ObjMetric extends Metric {
  var globalObj: Double = 0.0

  override def merge(other: Metric): Metric = {
    this.globalObj += other.asInstanceOf[ObjMetric].globalObj
    this
  }

  override def calculate: Double = {
    this.globalObj
  }

  override def setValues(values: Double*): Unit = {
    this.globalObj = values(0)
  }
}

object ObjMetric{
  def apply() = {
    new ObjMetric( )
  }
}
