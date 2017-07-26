package com.tencent.angel.ml.metrics

/**
  * Algorithm metric calculate interface
  */
trait Metric {
  def DEFAULT_METRIC_FORMAT = "%10.6e"

  /**
    * Set metric calculate dependency counters
    * @param values dependency counter values
    */
  def setValues(values:Double*)

  /**
    * Merge dependency counters
    * @param other
    * @return
    */
  def merge(other:Metric) : Metric

  /**
    * Use counters calculate metric
    * @return metric value
    */
  def calculate : Double

  def toString: String
}
