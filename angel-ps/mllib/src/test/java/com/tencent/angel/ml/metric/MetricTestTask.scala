/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.metric

import com.tencent.angel.exception.AngelException
import com.tencent.angel.worker.task.{BaseTask, TaskContext}

class MetricTestTask(val ctx: TaskContext) extends BaseTask[Long, Long, Long](ctx) {
  override def parse(key: Long, value: Long): Long = 10

  override def preProcess(taskContext: TaskContext): Unit = {}

  override def run(taskContext: TaskContext): Unit = {
    try {
      val globalMetrics: GlobalMetrics = GlobalMetrics(taskContext)
      globalMetrics.addMetric("loss", new LossMetric(10000000))
      while (taskContext.getEpoch < 10) {
        try {
          Thread.sleep(5000)
        }
        catch {
          case e: InterruptedException => {
          }
        }
        globalMetrics.metric("loss", 10000.0 / (1 + taskContext.getEpoch))
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
