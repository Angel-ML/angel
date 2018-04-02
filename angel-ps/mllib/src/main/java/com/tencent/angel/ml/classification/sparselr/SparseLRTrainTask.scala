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

package com.tencent.angel.ml.classification.sparselr

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

class SparseLRTrainTask(ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG = LogFactory.getLog(classOf[SparseLRTrainTask])
  val indexRange: Long = conf.getLong(ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  val modelSize: Long = conf.getLong(ML_MODEL_SIZE, indexRange)
  override val dataParser = DataParser(conf)

  override def train(ctx: TaskContext): Unit = {

    val epochNum = conf.getInt(ML_EPOCH_NUM, 5)
    val regParam = conf.getDouble(ML_LR_REG_L1, 0.001)
    val rho = conf.getDouble("rho", 0.01)
    val threadNum = conf.getInt(ANGEL_WORKER_THREAD_NUM, 1)

    LOG.info(s"Start training for SparseLR model with epochNum=$epochNum L1=$regParam rho=$rho")

    val learner = new SparseLRLearner(ctx)

    learner.setMaxIter(epochNum)
      .setRegParam(regParam)
      .setThreadNum(threadNum)
      .setRho(rho)

    learner.train(taskDataBlock, null)
  }

  /**
    * Parsing the input text to training data
    *
    * @param key   the key
    * @param value the value
    **/
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

}
