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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.factorizationmachines

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * This task train a Factorization Machines model
  *
  * @param ctx ： task context
  */
class FMTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG = LogFactory.getLog(classOf[FMTrainTask])
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  override val dataParser = DataParser(conf)

  private var minP = Double.MaxValue
  private var maxP = Double.MinValue

  override def train(taskContext: TaskContext): Unit = {
    LOG.info("FM train task.")
    taskDataBlock.shuffle()

    val learner = new FMLearner(ctx, minP, maxP)
    learner.train(taskDataBlock, null)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, line: Text): LabeledData = {
    dataParser.parse(line.toString)
  }

  override
  def preProcess(taskContext: TaskContext) {
    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val data = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (data != null) {
        taskDataBlock.put(data)

        minP = if (data.getY < minP) data.getY else minP
        maxP = if (data.getY > maxP) data.getY else maxP
      }
    }
    taskDataBlock.flush()
    LOG.info(s"Preprocessed ${taskDataBlock.size()} samples. minP=$minP, maxP=$maxP")
  }

}
