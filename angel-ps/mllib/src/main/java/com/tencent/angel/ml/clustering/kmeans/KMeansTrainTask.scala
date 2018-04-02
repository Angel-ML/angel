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

package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Kmeans clustering algorithm aims to partition sample points into n clusters, in which
  * each point belongs to the cluster with the nearest distance to the cluster center.
  */
class KMeansTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[KMeansTrainTask])

  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, -1L)
  assert(indexRange != -1L)
  override val dataParser = DataParser(conf)

  /**
    * Parse input text to trainning data
    *
    * @param key   the key type
    * @param value the value
    **/
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  override
  def train(ctx: TaskContext) = {
    LOG.info("#TrainSample=" + taskDataBlock.size)

    val learner = new KMeansLearner(ctx)
    taskDataBlock.shuffle()
    learner.train(taskDataBlock, null)

  }
}
