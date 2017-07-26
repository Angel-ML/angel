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
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Predict task of k-means model, first pull centers from PS, second predict the
  * value of local dataset
  *
  * @param ctx : context of current task
  */

class KMeansPredictTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[KMeansTrainTask])

  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val dataFormat: String = conf.get(MLConf.ML_DATAFORMAT)

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, "libsvm", false)
  }

  override def train(taskContext: TaskContext): Unit = {
    LOG.info("#PredictSample=" + trainDataBlock.size)

    val model = new KMeansModel(conf, ctx)

    // Predict the input dataset and save the result
    val predictResult = model.predict(trainDataBlock)
    LOG.info("predict storage.len=" + predictResult.size)

    HdfsUtil.writeStorage(predictResult, ctx)
  }
}
