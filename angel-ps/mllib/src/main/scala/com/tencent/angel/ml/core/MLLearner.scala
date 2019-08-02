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


package com.tencent.angel.ml.core


/**
  * Base class for all machine learning algorithm learner.
  */


import com.tencent.angel.ml.metric.GlobalMetrics
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.ml.model.OldMLModel
import com.tencent.angel.mlcore.{Learner, MLModel}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

abstract class MLLearner(val ctx: TaskContext) extends Learner {
  val globalMetrics = new GlobalMetrics(ctx)
  val conf: Configuration = ctx.getConf

  override protected def barrier(): Unit = {
    PSAgentContext.get().barrier(ctx.getTaskId.getIndex)
  }

  override def train(trainData: DataBlock[LabeledData],
                     validationData: DataBlock[LabeledData]): MLModel = ???

  def train(posTrainData: DataBlock[LabeledData],
            negTrainData: DataBlock[LabeledData],
            validationData: DataBlock[LabeledData]): MLModel = ???

  def trainOld(trainBlock: DataBlock[LabeledData],
               validBlock: DataBlock[LabeledData]): OldMLModel = ???
}