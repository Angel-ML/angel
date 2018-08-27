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


package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.ml.core.PredictTask
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

class KMeansPredictTask(ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[KMeansTrainTask])

  override def predict(ctx: TaskContext): Unit = {
    LOG.info("#PredictSample=" + taskDataBlock.size)

    val model = new KMeansModel(conf, ctx)
    predict(ctx, model, taskDataBlock)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
}