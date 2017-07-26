package com.tencent.angel.ml.metric.log

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.metrics.Metric
import com.tencent.angel.worker.task.TaskContext

import scala.collection.mutable

class GlobalMetrics(ctx: TaskContext) {

  val metricsTable = mutable.Map[String, Metric]()

  def metrics(metricName: String, metricValues: Double*) = {
    val metric:Option[Metric] = metricsTable.get(metricName)
    if(metric.isDefined) {
      metric.get.setValues(metricValues:_*)
    } else {
      throw new AngelException(s"Can not find metric $metricName in metrics table, " +
        s"you must register it use addMetrics first")
    }
  }

  def addMetrics(metricName: String, metric: Metric) = {
    metricsTable += (metricName -> metric)
    ctx.addAlgoMetric(metricName, metric)
  }
}

object GlobalMetrics {
  def apply(ctx: TaskContext): GlobalMetrics = new GlobalMetrics(ctx)
}
