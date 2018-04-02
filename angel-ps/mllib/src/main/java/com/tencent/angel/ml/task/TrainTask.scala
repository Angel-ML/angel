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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.ml.task

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.{BaseTask, TaskContext}

/**
  * The type labeled base task.
  */
abstract class TrainTask[KEYIN, VALUEIN](taskContext: TaskContext) extends BaseTask[KEYIN, VALUEIN, LabeledData](taskContext) {
  protected val dataParser = DataParser(conf)

  final def run(taskContext: TaskContext) = {
    this.train(taskContext)
  }

  def train(taskContext: TaskContext);
}
