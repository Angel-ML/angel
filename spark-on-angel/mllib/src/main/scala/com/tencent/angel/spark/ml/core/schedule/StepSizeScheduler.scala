package com.tencent.angel.spark.ml.core.schedule

trait StepSizeScheduler extends Serializable {

  def next(): Double

}
