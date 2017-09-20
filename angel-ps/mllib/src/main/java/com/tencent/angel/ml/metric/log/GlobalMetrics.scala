/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

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
