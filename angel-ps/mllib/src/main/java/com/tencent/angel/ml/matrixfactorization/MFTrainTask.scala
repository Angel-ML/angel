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

package com.tencent.angel.ml.matrixfactorization

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Train task that learns a matrix factorizaiton model
  *
  * @param ctx : the context of current running task
  */
class MFTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {

  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    null
  }

  override
  def preProcess(ctx: TaskContext): Unit = {

  }

  def train(ctx: TaskContext) {
    val learner = new MFLearner(ctx)
    learner.train(this.taskDataBlock, null)
  }
}