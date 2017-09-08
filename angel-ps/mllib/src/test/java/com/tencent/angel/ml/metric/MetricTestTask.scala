package com.tencent.angel.ml.metric

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.metric.log.{GlobalMetrics, LossMetric}
import com.tencent.angel.worker.task.{BaseTask, TaskContext}

class MetricTestTask(val ctx: TaskContext) extends BaseTask[Long, Long, Long](ctx) {
  override def parse(key: Long, value: Long): Long = 10

  override def preProcess(taskContext: TaskContext): Unit = {}

  override def run(taskContext: TaskContext): Unit = {
    try {
      val globalMetrics: GlobalMetrics = GlobalMetrics(taskContext)
      globalMetrics.addMetrics("loss", new LossMetric(10000000))
      while (taskContext.getEpoch < 10) {
        try {
          Thread.sleep(5000)
        }
        catch {
          case e: InterruptedException => {
          }
        }
        globalMetrics.metrics("loss", 10000.0 / (1 + taskContext.getEpoch))
        taskContext.incEpoch()
      }
    }
    catch {
      case x: Throwable => {
        throw new AngelException("run task failed ", x)
      }
    }
  }
}
