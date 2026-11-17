package com.tencent.angel.spark.ml.time_series.stat

import org.apache.spark.ml.param.{Param, Params}


trait TimeSeriesParams extends Params {
  final val timeCol = new Param[String](this, "timeCol",
    "The column that stored time value")
  def setTimeCol(value: String): this.type = set(timeCol, value)

  final val timeSeriesCol = new Param[String](this, "timeSeriesCol",
    "The column that stored time series value")
  def setTimeSeriesCol(value: String): this.type = set(timeSeriesCol, value)

  final val exogenousCol = new Param[String](this, "exogenousCol",
    "The column that stored exogenous value")
  def setExogenousCol(value: String): this.type = set(exogenousCol, value)
}
